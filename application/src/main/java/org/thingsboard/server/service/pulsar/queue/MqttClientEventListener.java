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
package org.thingsboard.server.service.pulsar.queue;

import io.triveni.broker.mqtt.message.MqttClientEvent;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MqttClientEventListener implements MessageListener<MqttClientEvent> {

    private static final Logger log = LoggerFactory.getLogger(MqttClientEventListener.class);

    private final MqttClientEventService mqttClientEventService;

    public MqttClientEventListener(MqttClientEventService mqttClientEventService) {
        this.mqttClientEventService = mqttClientEventService;
    }

    @Override
    public void received(Consumer<MqttClientEvent> consumer, Message<MqttClientEvent> message) {
        mqttClientEventService.process(message, consumer);
    }

    @Override
    public void reachedEndOfTopic(Consumer<MqttClientEvent> consumer) {

    }
}
