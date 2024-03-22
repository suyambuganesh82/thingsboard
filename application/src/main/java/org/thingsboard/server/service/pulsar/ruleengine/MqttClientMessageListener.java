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

import io.triveni.broker.mqtt.message.MqttClientMessage;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MqttClientMessageListener implements MessageListener<MqttClientMessage> {

    private static final Logger log = LoggerFactory.getLogger(MqttClientMessageListener.class);

    private final PulsarRuleEngineService pulsarRuleEngineService;

    public MqttClientMessageListener(PulsarRuleEngineService pulsarRuleEngineService) {
        this.pulsarRuleEngineService = pulsarRuleEngineService;
    }

    @Override
    public void received(Consumer<MqttClientMessage> consumer, Message<MqttClientMessage> message) {
        pulsarRuleEngineService.process(message, consumer);
    }

    @Override
    public void reachedEndOfTopic(Consumer<MqttClientMessage> consumer) {

    }
}
