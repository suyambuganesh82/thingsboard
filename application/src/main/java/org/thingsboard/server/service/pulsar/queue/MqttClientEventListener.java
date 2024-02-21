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
