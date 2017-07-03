package io.zeebe.test.broker.protocol.clientapi;

import static io.zeebe.protocol.clientapi.ExecuteCommandRequestEncoder.keyNullValue;
import static io.zeebe.protocol.clientapi.ExecuteCommandRequestEncoder.partitionIdNullValue;
import static io.zeebe.util.StringUtil.getBytes;

import java.util.Map;

import org.agrona.MutableDirectBuffer;
import io.zeebe.protocol.clientapi.EventType;
import io.zeebe.protocol.clientapi.ExecuteCommandRequestEncoder;
import io.zeebe.protocol.clientapi.ExecuteCommandResponseEncoder;
import io.zeebe.protocol.clientapi.MessageHeaderEncoder;
import io.zeebe.test.broker.protocol.MsgPackHelper;
import io.zeebe.transport.requestresponse.client.TransportConnectionPool;
import io.zeebe.util.buffer.BufferWriter;

public class ExecuteCommandRequest implements BufferWriter
{
    protected final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    protected final ExecuteCommandRequestEncoder requestEncoder = new ExecuteCommandRequestEncoder();
    protected final MsgPackHelper msgPackHelper;

    protected String topicName;
    protected int partitionId = partitionIdNullValue();
    protected long key = keyNullValue();
    protected EventType eventType = EventType.NULL_VAL;
    protected byte[] encodedCmd;

    protected final RequestResponseExchange requestResponseExchange;


    public ExecuteCommandRequest(final TransportConnectionPool connectionPool, final int channelId, final MsgPackHelper msgPackHelper)
    {
        this.requestResponseExchange = new RequestResponseExchange(connectionPool, channelId);
        this.msgPackHelper = msgPackHelper;
    }

    public ExecuteCommandRequest topicName(final String topicName)
    {
        this.topicName = topicName;
        return this;
    }

    public ExecuteCommandRequest partitionId(final int partitionId)
    {
        this.partitionId = partitionId;
        return this;
    }

    public ExecuteCommandRequest key(final long key)
    {
        this.key = key;
        return this;
    }

    public ExecuteCommandRequest eventType(final EventType eventType)
    {
        this.eventType = eventType;
        return this;
    }

    public ExecuteCommandRequest command(final Map<String, Object> command)
    {
        this.encodedCmd = msgPackHelper.encodeAsMsgPack(command);
        return this;
    }

    public ExecuteCommandRequest send()
    {
        requestResponseExchange.sendRequest(this);
        return this;
    }

    public ExecuteCommandResponse await()
    {
        final ExecuteCommandResponse response = new ExecuteCommandResponse(msgPackHelper);
        requestResponseExchange.awaitResponse(response);
        return response;
    }

    public ErrorResponse awaitError()
    {
        final ErrorResponse errorResponse = new ErrorResponse(msgPackHelper);
        requestResponseExchange.awaitResponse(errorResponse);
        return errorResponse;
    }

    @Override
    public int getLength()
    {
        return MessageHeaderEncoder.ENCODED_LENGTH +
                ExecuteCommandRequestEncoder.BLOCK_LENGTH +
                ExecuteCommandResponseEncoder.topicNameHeaderLength() +
                getBytes(topicName).length +
                ExecuteCommandRequestEncoder.commandHeaderLength() +
                encodedCmd.length;
    }

    @Override
    public void write(final MutableDirectBuffer buffer, final int offset)
    {
        messageHeaderEncoder.wrap(buffer, offset)
            .schemaId(requestEncoder.sbeSchemaId())
            .templateId(requestEncoder.sbeTemplateId())
            .blockLength(requestEncoder.sbeBlockLength())
            .version(requestEncoder.sbeSchemaVersion());

        requestEncoder.wrap(buffer, offset + messageHeaderEncoder.encodedLength())
            .partitionId(partitionId)
            .key(key)
            .eventType(eventType)
            .topicName(topicName)
            .putCommand(encodedCmd, 0, encodedCmd.length);
    }

}