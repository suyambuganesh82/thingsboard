/**
 * Copyright © 2016-2024 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.server.service.pulsar.ruleengine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.triveni.broker.mqtt.message.MqttClientMessage;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.springframework.pulsar.listener.Acknowledgement;
import org.springframework.stereotype.Component;
import org.thingsboard.server.common.data.id.*;
import org.thingsboard.server.common.data.msg.TbMsgType;
import org.thingsboard.server.common.msg.TbMsg;
import org.thingsboard.server.common.msg.TbMsgDataType;
import org.thingsboard.server.common.msg.TbMsgMetaData;
import org.thingsboard.server.common.msg.TbMsgProcessingCtx;
import org.thingsboard.server.common.msg.queue.QueueToRuleEngineMsg;
import org.thingsboard.server.common.msg.queue.TbMsgCallback;
import org.thingsboard.server.service.queue.ruleengine.TbRuleEngineConsumerContext;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

@Component
@Slf4j
public class PulsarRuleEngineService {

    private final ObjectMapper objectMapper;
    private final TbRuleEngineConsumerContext ctx;

    public PulsarRuleEngineService(ObjectMapper objectMapper, TbRuleEngineConsumerContext ctx) {
        this.objectMapper = objectMapper;
        this.ctx = ctx;
    }

    public void process(Message<MqttClientMessage> message, Consumer<MqttClientMessage> consumer) {
        log.info("Received MQTT message with id: {}", message.getMessageId());
        MqttClientMessage mqttMessage = message.getValue();
        UUID id = mqttMessage.id();
        TenantId tenantId = TenantId.fromUUID(mqttMessage.tenantId().get());
        TbMsgCallback callback = new TbMsgRuleEngineCallback(id, tenantId, consumer, message.getMessageId());
        TbMsg tbMsg = createTbMsg(mqttMessage, id, callback);
        Set<String> relationTypes = new HashSet<>();
        QueueToRuleEngineMsg queueToRuleEngineMsg = new QueueToRuleEngineMsg(tenantId, tbMsg, relationTypes, "FAILURE MESSAGE");
        ctx.getActorContext().tell(queueToRuleEngineMsg);
        log.info("Published to RuleEngineActor: {}", id);
    }

    @SneakyThrows
    private TbMsg createTbMsg(MqttClientMessage message, UUID id, TbMsgCallback callback) {
        TbMsgMetaData metaData = new TbMsgMetaData(message.metaData());
        CustomerId customerId = message.customerId().isPresent() ? new CustomerId(message.customerId().get()) : null;
        RuleChainId ruleChainId = message.ruleChainId().isPresent() ? new RuleChainId(message.ruleChainId().get()) : null;
        RuleNodeId ruleNodeId = message.ruleNodeId().isPresent() ? new RuleNodeId(message.ruleNodeId().get()) : null;
        DeviceId entityId = message.deviceId().isPresent() ? new DeviceId(message.deviceId().get()) : null;

        TbMsgDataType dataType = TbMsgDataType.JSON;
        TbMsgType tbMsgType = TbMsgType.POST_TELEMETRY_REQUEST;
        String tbMsgTypeString = TbMsgType.POST_TELEMETRY_REQUEST.name();
        String data = "";
        if (message.payload().isPresent()) {
            JsonNode jsonNode =  objectMapper.readTree(message.payload().get());
            data = jsonNode.toString();
        }
        TbMsgProcessingCtx ctx = new TbMsgProcessingCtx();
        return new TbMsg("queueName", id, message.createdTime(), tbMsgType, tbMsgTypeString, entityId, customerId,
                metaData, dataType, data, ruleChainId, ruleNodeId, ctx, callback);
    }
}
