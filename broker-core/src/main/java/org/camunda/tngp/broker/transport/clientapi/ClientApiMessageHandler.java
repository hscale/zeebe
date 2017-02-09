package org.camunda.tngp.broker.transport.clientapi;

import static org.camunda.tngp.transport.protocol.Protocols.FULL_DUPLEX_SINGLE_MESSAGE;
import static org.camunda.tngp.transport.protocol.Protocols.REQUEST_RESPONSE;

import java.util.function.Consumer;

import org.agrona.DirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.agrona.concurrent.UnsafeBuffer;
import org.camunda.tngp.broker.Constants;
import org.camunda.tngp.broker.logstreams.BrokerEventMetadata;
import org.camunda.tngp.dispatcher.Dispatcher;
import org.camunda.tngp.logstreams.log.LogStream;
import org.camunda.tngp.logstreams.log.LogStreamWriter;
import org.camunda.tngp.protocol.clientapi.ControlMessageRequestDecoder;
import org.camunda.tngp.protocol.clientapi.ControlMessageRequestEncoder;
import org.camunda.tngp.protocol.clientapi.ErrorCode;
import org.camunda.tngp.protocol.clientapi.EventType;
import org.camunda.tngp.protocol.clientapi.ExecuteCommandRequestDecoder;
import org.camunda.tngp.protocol.clientapi.MessageHeaderDecoder;
import org.camunda.tngp.protocol.clientapi.MessageHeaderEncoder;
import org.camunda.tngp.transport.TransportChannel;
import org.camunda.tngp.transport.protocol.TransportHeaderDescriptor;
import org.camunda.tngp.transport.requestresponse.RequestResponseProtocolHeaderDescriptor;
import org.camunda.tngp.transport.singlemessage.SingleMessageHeaderDescriptor;

public class ClientApiMessageHandler
{

    protected final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    protected final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    protected final TransportHeaderDescriptor transportHeaderDescriptor = new TransportHeaderDescriptor();
    protected final RequestResponseProtocolHeaderDescriptor requestResponseProtocolHeaderDescriptor = new RequestResponseProtocolHeaderDescriptor();
    protected final ExecuteCommandRequestDecoder executeCommandRequestDecoder = new ExecuteCommandRequestDecoder();
    protected final ControlMessageRequestEncoder controlMessageRequestEncoder = new ControlMessageRequestEncoder();

    protected final UnsafeBuffer writeControlMessageRequestBufferView = new UnsafeBuffer(0, 0);

    protected final ManyToOneConcurrentArrayQueue<Runnable> cmdQueue = new ManyToOneConcurrentArrayQueue<>(100);
    protected final Consumer<Runnable> cmdConsumer = (c) -> c.run();

    protected final Long2ObjectHashMap<LogStream> logStreamsById = new Long2ObjectHashMap<>();
    protected final BrokerEventMetadata eventMetadata = new BrokerEventMetadata();
    protected final LogStreamWriter logStreamWriter = new LogStreamWriter();

    protected final Dispatcher sendBuffer;
    protected final Dispatcher controlMessageDispatcher;
    protected final ErrorResponseWriter errorResponseWriter;

    public ClientApiMessageHandler(Dispatcher sendBuffer, Dispatcher controlMessageDispatcher, ErrorResponseWriter errorResponseWriter)
    {
        this.sendBuffer = sendBuffer;
        this.controlMessageDispatcher = controlMessageDispatcher;
        this.errorResponseWriter = errorResponseWriter;
    }

    public boolean handleMessage(TransportChannel transportChannel, DirectBuffer buffer, int offset, int length)
    {
        boolean isHandled = false;

        cmdQueue.drain(cmdConsumer);

        eventMetadata.reset();
        eventMetadata.reqChannelId(transportChannel.getId());

        int messageOffset = offset + TransportHeaderDescriptor.headerLength();
        int messageLength = length - TransportHeaderDescriptor.headerLength();

        transportHeaderDescriptor.wrap(buffer, offset);

        final int protocol = transportHeaderDescriptor.protocolId();
        if (protocol == REQUEST_RESPONSE)
        {
            requestResponseProtocolHeaderDescriptor.wrap(buffer, messageOffset);

            eventMetadata.reqConnectionId(requestResponseProtocolHeaderDescriptor.connectionId());
            eventMetadata.reqRequestId(requestResponseProtocolHeaderDescriptor.requestId());
            messageOffset += RequestResponseProtocolHeaderDescriptor.headerLength();
            messageLength -= RequestResponseProtocolHeaderDescriptor.headerLength();
        }
        else if (protocol == FULL_DUPLEX_SINGLE_MESSAGE)
        {
            messageOffset += SingleMessageHeaderDescriptor.HEADER_LENGTH;
            messageLength -= SingleMessageHeaderDescriptor.HEADER_LENGTH;
        }

        messageHeaderDecoder.wrap(buffer, messageOffset);

        final int templateId = messageHeaderDecoder.templateId();

        messageOffset += messageHeaderDecoder.encodedLength();
        messageLength -= messageHeaderDecoder.encodedLength();

        final int clientVersion = messageHeaderDecoder.version();

        if (clientVersion > Constants.PROTOCOL_VERSION)
        {
            return errorResponseWriter
                        .metadata(eventMetadata)
                        .errorCode(ErrorCode.INVALID_CLIENT_VERSION)
                        .errorMessage("Client has newer version than broker (%d > %d)", clientVersion, Constants.PROTOCOL_VERSION)
                        .failedRequest(buffer, messageOffset, messageLength)
                        .tryWriteResponseOrLogFailure();
        }

        eventMetadata.protocolVersion(clientVersion);

        switch (templateId)
        {
            case ExecuteCommandRequestDecoder.TEMPLATE_ID:

                isHandled = handleExecuteCommandRequest(eventMetadata, buffer, messageOffset, messageLength);
                break;

            case ControlMessageRequestDecoder.TEMPLATE_ID:

                isHandled = handleControlMessageRequest(eventMetadata, buffer, messageOffset, messageLength);
                break;

            default:
                isHandled = errorResponseWriter
                        .metadata(eventMetadata)
                        .errorCode(ErrorCode.MESSAGE_NOT_SUPPORTED)
                        .errorMessage("Cannot handle message. Template id '%d' is not supported.", templateId)
                        .failedRequest(buffer, messageOffset, messageLength)
                        .tryWriteResponseOrLogFailure();
                break;
        }

        return isHandled;
    }

    protected boolean handleExecuteCommandRequest(BrokerEventMetadata eventMetadata, DirectBuffer buffer, int messageOffset, int messageLength)
    {
        boolean isHandled = false;

        executeCommandRequestDecoder.wrap(buffer, messageOffset, messageHeaderDecoder.blockLength(), messageHeaderDecoder.version());

        final long topicId = executeCommandRequestDecoder.topicId();
        final long longKey = executeCommandRequestDecoder.longKey();

        final EventType eventType = executeCommandRequestDecoder.eventType();
        eventMetadata.eventType(eventType);

        final int eventOffset = executeCommandRequestDecoder.limit() + ExecuteCommandRequestDecoder.commandHeaderLength();
        final int eventLength = executeCommandRequestDecoder.commandLength();

        final LogStream logStream = logStreamsById.get(topicId);

        if (logStream != null)
        {
            // TODO: eventMetadata.raftTermId(logStream.getCurrentTermId());

            logStreamWriter.wrap(logStream);

            if (longKey != ExecuteCommandRequestDecoder.longKeyNullValue())
            {
                logStreamWriter.key(longKey);
            }
            else
            {
                logStreamWriter.positionAsKey();
            }

            final long eventPosition = logStreamWriter
                .metadataWriter(eventMetadata)
                .value(buffer, eventOffset, eventLength)
                .tryWrite();

            isHandled = eventPosition >= 0;

            if (!isHandled)
            {
                isHandled = errorResponseWriter
                        .metadata(eventMetadata)
                        .errorCode(ErrorCode.REQUEST_WRITE_FAILURE)
                        .errorMessage("Failed to write execute command request.")
                        .failedRequest(buffer, messageOffset, messageLength)
                        .tryWriteResponseOrLogFailure();
            }
        }
        else
        {
            isHandled = errorResponseWriter
                    .metadata(eventMetadata)
                    .errorCode(ErrorCode.TOPIC_NOT_FOUND)
                    .errorMessage("Cannot execute command. Topic with id '%d' not found", topicId)
                    .failedRequest(buffer, messageOffset, messageLength)
                    .tryWriteResponseOrLogFailure();
        }
        return isHandled;
    }

    protected boolean handleControlMessageRequest(BrokerEventMetadata eventMetadata, DirectBuffer buffer, int messageOffset, int messageLength)
    {
        boolean isHandled = false;
        long publishPosition = -1;

        do
        {
            writeControlMessageRequestBufferView.wrap(buffer);

            controlMessageRequestEncoder
                .wrap(writeControlMessageRequestBufferView, messageOffset)
                .reqChannelId(eventMetadata.getReqChannelId())
                .reqConnectionId(eventMetadata.getReqConnectionId())
                .reqRequestId(eventMetadata.getReqRequestId());

            publishPosition = controlMessageDispatcher.offer(buffer, messageOffset, messageLength);
        }
        while (publishPosition == -2);

        isHandled = publishPosition >= 0;

        if (!isHandled)
        {
            isHandled = errorResponseWriter
                .metadata(eventMetadata)
                .errorCode(ErrorCode.REQUEST_WRITE_FAILURE)
                .errorMessage("Failed to write control message request.")
                .failedRequest(buffer, messageOffset, messageLength)
                .tryWriteResponseOrLogFailure();
        }
        return isHandled;
    }

    public void addStream(LogStream logStream)
    {
        cmdQueue.add(() -> logStreamsById.put(logStream.getId(), logStream));
    }

    public void removeStream(LogStream logStream)
    {
        cmdQueue.add(() -> logStreamsById.remove(logStream.getId()));
    }
}