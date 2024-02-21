/**
 * Copyright © 2016-2024 The Thingsboard Authors
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.server.service.pulsar.ruleengine;

import io.micrometer.core.instrument.Timer;
import io.triveni.broker.mqtt.message.MqttClientMessage;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.springframework.pulsar.listener.Acknowledgement;
import org.thingsboard.common.util.ExceptionUtil;
import org.thingsboard.server.common.data.exception.AbstractRateLimitException;
import org.thingsboard.server.common.data.id.RuleNodeId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.msg.queue.RuleEngineException;
import org.thingsboard.server.common.msg.queue.RuleNodeInfo;
import org.thingsboard.server.common.msg.queue.TbMsgCallback;

import java.time.Clock;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Slf4j
public class TbMsgRuleEngineCallback implements TbMsgCallback {

    private final UUID id;
    private final MessageId messageId;
    private final TenantId tenantId;
    private final Consumer<MqttClientMessage> consumer;
    private final long startMsgProcessing;
    private final Timer successfulMsgTimer;
    private final Timer failedMsgTimer;

    public TbMsgRuleEngineCallback(UUID id, TenantId tenantId, Consumer<MqttClientMessage> consumer, MessageId messageId) {
        this(id, messageId, tenantId, consumer, null, null);
    }

    public TbMsgRuleEngineCallback(UUID id, MessageId messageId, TenantId tenantId, Consumer<MqttClientMessage> consumer, Timer successfulMsgTimer, Timer failedMsgTimer) {
        this.id = id;
        this.messageId = messageId;
        this.tenantId = tenantId;
        this.consumer = consumer;
        this.successfulMsgTimer = successfulMsgTimer;
        this.failedMsgTimer = failedMsgTimer;
        startMsgProcessing = Clock.systemUTC().millis();
    }

    @SneakyThrows
    @Override
    public void onSuccess() {
        log.debug("[{}] ON SUCCESS", id);
        if (successfulMsgTimer != null) {
            successfulMsgTimer.record(System.currentTimeMillis() - startMsgProcessing, TimeUnit.MILLISECONDS);
        }
        consumer.acknowledge(messageId);
//        ctx.onSuccess(id);
    }

    @SneakyThrows
    @Override
    public void onRateLimit(RuleEngineException e) {
        log.debug("[{}] ON RATE LIMIT", id, e);
        //TODO notify tenant on rate limit
        if (failedMsgTimer != null) {
            failedMsgTimer.record(System.currentTimeMillis() - startMsgProcessing, TimeUnit.MILLISECONDS);
        }
        consumer.acknowledge(messageId);
//        ctx.onSuccess(id);
    }

    @Override
    public void onFailure(RuleEngineException e) {
        if (ExceptionUtil.lookupExceptionInCause(e, AbstractRateLimitException.class) != null) {
            onRateLimit(e);
            return;
        }

        log.debug("[{}] ON FAILURE", id, e);
        if (failedMsgTimer != null) {
            failedMsgTimer.record(System.currentTimeMillis() - startMsgProcessing, TimeUnit.MILLISECONDS);
        }
//        ctx.onFailure(tenantId, id, e);
        consumer.negativeAcknowledge(messageId);
    }

    @Override
    public boolean isMsgValid() {
        return true;
//        return !ctx.isCanceled();
    }

    @Override
    public void onProcessingStart(RuleNodeInfo ruleNodeInfo) {
        log.debug("[{}] ON PROCESSING START: {}", id, ruleNodeInfo);
//        ctx.onProcessingStart(id, ruleNodeInfo);
    }

    @Override
    public void onProcessingEnd(RuleNodeId ruleNodeId) {
        log.debug("[{}] ON PROCESSING END: {}", id, ruleNodeId);
//        ctx.onProcessingEnd(id, ruleNodeId);
    }
}
