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
