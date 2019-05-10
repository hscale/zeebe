/*
 * Zeebe Broker Core
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.broker.subscription.message.processor;

import io.zeebe.broker.logstreams.processor.SideEffectProducer;
import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.logstreams.processor.TypedRecordProcessor;
import io.zeebe.broker.logstreams.processor.TypedResponseWriter;
import io.zeebe.broker.logstreams.processor.TypedStreamWriter;
import io.zeebe.broker.subscription.command.SubscriptionCommandSender;
import io.zeebe.broker.subscription.message.data.MessageSubscriptionRecord;
import io.zeebe.broker.subscription.message.state.MessageState;
import io.zeebe.broker.subscription.message.state.MessageSubscription;
import io.zeebe.broker.subscription.message.state.MessageSubscriptionState;
import io.zeebe.protocol.clientapi.RejectionType;
import java.util.function.Consumer;

public class ResetMessageCorrelationProcessor
    implements TypedRecordProcessor<MessageSubscriptionRecord> {

  private final MessageState messageState;
  private final MessageSubscriptionState subscriptionState;
  private final MessageSubscriptionRecord correlateMessageRecord = new MessageSubscriptionRecord();
  private final MessageCorrelator messageCorrelator;

  public ResetMessageCorrelationProcessor(
      final MessageState messageState,
      final MessageSubscriptionState subscriptionState,
      final SubscriptionCommandSender subscriptionCommandSender) {
    this.messageState = messageState;
    this.subscriptionState = subscriptionState;
    this.messageCorrelator =
        new MessageCorrelator(messageState, subscriptionState, subscriptionCommandSender);
  }

  @Override
  public void processRecord(
      final TypedRecord<MessageSubscriptionRecord> record,
      final TypedResponseWriter responseWriter,
      final TypedStreamWriter streamWriter,
      final Consumer<SideEffectProducer> sideEffect) {

    final MessageSubscriptionRecord subscriptionRecord = record.getValue();
    final long messageKey = record.getValue().getMessageKey();
    final long workflowInstanceKey = record.getValue().getWorkflowInstanceKey();

    if (!messageState.existMessageCorrelation(messageKey, workflowInstanceKey)) {
      streamWriter.appendRejection(
          record,
          RejectionType.INVALID_STATE,
          String.format(
              "Expected message %d to be correlated in workflow instance %d but no correlation was found",
              messageKey, workflowInstanceKey));
      return;
    }
    messageState.removeMessageCorrelation(messageKey, workflowInstanceKey);

    subscriptionState.visitSubscriptions(
        subscriptionRecord.getMessageName(),
        subscriptionRecord.getCorrelationKey(),
        subscription -> {
          if (!subscription.isCorrelating()) {
            correlateMessage(streamWriter, subscription, sideEffect);
            return false;
          }
          return true;
        });
  }

  private void correlateMessage(
      final TypedStreamWriter streamWriter,
      final MessageSubscription subscription,
      final Consumer<SideEffectProducer> sideEffect) {
    correlateMessageRecord.reset();
    correlateMessageRecord
        .setMessageKey(subscription.getMessageKey())
        .setWorkflowInstanceKey(subscription.getWorkflowInstanceKey())
        .setElementInstanceKey(subscription.getElementInstanceKey())
        .setCorrelationKey(subscription.getCorrelationKey())
        .setMessageName(subscription.getMessageName());

    messageCorrelator.correlateNextMessage(subscription, correlateMessageRecord, sideEffect);
  }
}
