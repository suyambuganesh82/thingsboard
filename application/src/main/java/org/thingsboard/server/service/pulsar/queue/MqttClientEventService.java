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
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.springframework.stereotype.Component;
import org.thingsboard.server.actors.ActorSystemContext;
import org.thingsboard.server.common.msg.queue.TbCallback;
import org.thingsboard.server.service.pulsar.queue.msg.MqttClientEventToDeviceActorMsg;

@Component
@Slf4j
public class MqttClientEventService {

    private final ActorSystemContext actorContext;

    public MqttClientEventService(ActorSystemContext actorContext) {
        this.actorContext = actorContext;
    }

    public void process(Message<MqttClientEvent> message, Consumer<MqttClientEvent> consumer) {
        log.info("Received MqttClientEvent message with id: {}", message.getValue().id());
        TbCallback callback = new TbCallback() {
            @Override
            public void onSuccess() {
                log.debug("Processed MqttClientEvent message with id: [{}]", message.getValue().id());
                consumer.acknowledgeAsync(message.getMessageId());
            }

            @Override
            public void onFailure(Throwable t) {
                log.debug("Failed to process MqttClientEvent message with id: [{}]", message.getValue().id(), t);
                consumer.acknowledgeAsync(message.getMessageId()); //still ack, this is an activity message
            }
        };
        MqttClientEventToDeviceActorMsg msg = new MqttClientEventToDeviceActorMsg(message.getValue(), callback);
        actorContext.tell(msg);
    }
}
