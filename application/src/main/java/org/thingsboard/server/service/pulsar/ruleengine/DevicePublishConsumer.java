package org.thingsboard.server.service.pulsar.ruleengine;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.triveni.broker.mqtt.message.MqttClientMessage;
import io.triveni.platform.rpc.common.JacksonJsonReader;
import io.triveni.platform.rpc.common.JacksonJsonWriter;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.api.schema.SchemaReader;
import org.apache.pulsar.client.api.schema.SchemaWriter;
import org.apache.pulsar.client.impl.schema.SchemaDefinitionBuilderImpl;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.springframework.pulsar.core.PulsarAdministration;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.pulsar.core.PulsarTopic;
import org.springframework.stereotype.Service;
import org.thingsboard.server.queue.pulsar.config.PulsarConfig;

@Service
@Slf4j
public class DevicePublishConsumer {

    private final PulsarConfig pulsarConfig;
    private final PulsarAdministration pulsarAdministration;
    private final PulsarClient pulsarClient;
    private final PulsarTemplate<MqttClientMessage> pulsarTemplate;
    private final PulsarRuleEngineService pulsarRuleEngineService;
    private final ObjectMapper objectMapper;

    private boolean topicCreated = false;
    private String topic;
    private String subscription;

    private Consumer<MqttClientMessage> mqttMessageConsumer = null;

    public DevicePublishConsumer(PulsarConfig pulsarConfig, PulsarAdministration pulsarAdministration,
                                 PulsarClient pulsarClient, PulsarTemplate<MqttClientMessage> pulsarTemplate,
                                 PulsarRuleEngineService pulsarRuleEngineService, ObjectMapper objectMapper) {
        this.pulsarConfig = pulsarConfig;
        this.pulsarAdministration = pulsarAdministration;
        this.pulsarClient = pulsarClient;
        this.pulsarTemplate = pulsarTemplate;
        this.pulsarRuleEngineService = pulsarRuleEngineService;
        this.objectMapper = objectMapper;
    }

    @SneakyThrows
    @PostConstruct
    public void init() {
        if (!topicCreated && pulsarAdministration != null) {
            topic = pulsarConfig.getMqttDevicePublishTopic();
            int partition = pulsarConfig.getMqttDevicePublishPartition();
            subscription = pulsarConfig.getMqttDevicePublishSubscription();
            log.debug("Creating topic: [{}]", topic);
            RetentionPolicies policies = new RetentionPolicies(-1, -1); //infinite retention on namespace level
            try (PulsarAdmin adminClient = pulsarAdministration.createAdminClient()) {
                adminClient.namespaces().setRetention("public/default", policies);
            } catch (PulsarAdminException e) {
                log.error("Creating topic failed: [{}]", topic, e);
                throw new RuntimeException(e);
            }
            PulsarTopic pulsarTopic = PulsarTopic.builder(topic)
                    .numberOfPartitions(partition)
                    .build();
            pulsarAdministration.createOrModifyTopics(pulsarTopic);
            topicCreated = true;
        }

        SchemaReader<MqttClientMessage> reader = new JacksonJsonReader<>(objectMapper, MqttClientMessage.class);
        SchemaWriter<MqttClientMessage> writer = new JacksonJsonWriter<>(objectMapper);
        SchemaDefinition<MqttClientMessage> schemaDefinition = new SchemaDefinitionBuilderImpl<MqttClientMessage>()
                .withPojo(MqttClientMessage.class)
                .withSchemaReader(reader)
                .withSchemaWriter(writer)
                .build();
        Schema<MqttClientMessage> mqttMessageSchema = Schema.JSON(schemaDefinition);
        MqttClientMessageListener mqttClientMessageListener = new MqttClientMessageListener(pulsarRuleEngineService);

        mqttMessageConsumer = pulsarClient.newConsumer(mqttMessageSchema)
                .topic(topic)
                // Allow multiple consumers to attach to the same subscription
                // and get messages dispatched as a queue
                .subscriptionType(SubscriptionType.Key_Shared)
                .subscriptionName(subscription)
                .receiverQueueSize(1)
                .messageListener(mqttClientMessageListener)
                .subscribe();

    }

    public void subscribe() {

    }

    @SneakyThrows
    @PreDestroy
    public void cleanUp() {
        if (null != mqttMessageConsumer && mqttMessageConsumer.isConnected()) {
            mqttMessageConsumer.close();
        }
    }

    //    @PulsarListener(consumerCustomizer = "devicePublishConsumerConfig",
//    @PulsarListener(topics = "mqtt-device-publish",
//            subscriptionName = "mqtt-device-publish-subscription",
//            subscriptionType= SubscriptionType.Key_Shared,
//            ackMode = AckMode.MANUAL, schemaType = SchemaType.JSON,
//            concurrency = "5")
//    void listen(Message<MqttMessage> message, Acknowledgement acknowledgement) {
//        pulsarRuleEngineService.process(message, acknowledgement);
//    }

}
