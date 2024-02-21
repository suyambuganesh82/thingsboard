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
