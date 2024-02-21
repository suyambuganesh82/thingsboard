package org.thingsboard.server.service.pulsar.queue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.triveni.broker.mqtt.message.MqttClientEvent;
import io.triveni.platform.rpc.common.JacksonJsonReader;
import io.triveni.platform.rpc.common.JacksonJsonWriter;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.api.schema.SchemaReader;
import org.apache.pulsar.client.api.schema.SchemaWriter;
import org.apache.pulsar.client.impl.schema.SchemaDefinitionBuilderImpl;
import org.springframework.pulsar.core.PulsarAdministration;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.pulsar.core.PulsarTopic;
import org.springframework.stereotype.Service;
import org.thingsboard.server.queue.pulsar.config.PulsarConfig;

@Service
@Slf4j
public class MqttClientEventConsumer {

    private final PulsarConfig pulsarConfig;
    private final PulsarAdministration pulsarAdministration;
    private final PulsarClient pulsarClient;
    private final PulsarTemplate<MqttClientEvent> pulsarTemplate;
    private final MqttClientEventService service;
    private final ObjectMapper objectMapper;

    private boolean topicCreated = false;
    private String topic;
    private String subscription;
    private int partition;

    private Consumer<MqttClientEvent> consumer = null;

    public MqttClientEventConsumer(PulsarConfig pulsarConfig, PulsarAdministration pulsarAdministration,
                                   PulsarClient pulsarClient, PulsarTemplate<MqttClientEvent> pulsarTemplate,
                                   MqttClientEventService mqttClientEventService, ObjectMapper objectMapper) {
        this.pulsarConfig = pulsarConfig;
        this.pulsarAdministration = pulsarAdministration;
        this.pulsarClient = pulsarClient;
        this.pulsarTemplate = pulsarTemplate;
        this.service = mqttClientEventService;
        this.objectMapper = objectMapper;
    }

    @SneakyThrows
    @PostConstruct
    public void init() {
        if (!topicCreated && pulsarAdministration != null) {
            topic = pulsarConfig.getMqttClientEventTopic();
            subscription = pulsarConfig.getMqttClientEventSubscription();
            partition = pulsarConfig.getMqttClientEventPartition();
            log.debug("Creating topic: [{}]", topic);
//            RetentionPolicies policies = new RetentionPolicies(-1, -1); //infinite retention on namespace level
//            try (PulsarAdmin adminClient = pulsarAdministration.createAdminClient()) {
//                adminClient.namespaces().setRetention("public/default", policies);
//            } catch (PulsarAdminException e) {
//                log.error("Creating topic failed: [{}]", topic, e);
//                throw new RuntimeException(e);
//            }
            PulsarTopic pulsarTopic = PulsarTopic.builder(topic)
                    .numberOfPartitions(partition)
                    .build();
            pulsarAdministration.createOrModifyTopics(pulsarTopic);
            topicCreated = true;
        }

        SchemaReader<MqttClientEvent> reader = new JacksonJsonReader<>(objectMapper, MqttClientEvent.class);
        SchemaWriter<MqttClientEvent> writer = new JacksonJsonWriter<>(objectMapper);
        SchemaDefinition<MqttClientEvent> schemaDefinition = new SchemaDefinitionBuilderImpl<MqttClientEvent>()
                .withPojo(MqttClientEvent.class)
                .withSchemaReader(reader)
                .withSchemaWriter(writer)
                .build();
        Schema<MqttClientEvent> schema = Schema.JSON(schemaDefinition);
        MqttClientEventListener listener = new MqttClientEventListener(service);

        consumer = pulsarClient.newConsumer(schema)
                .topic(topic)
                // Allow multiple consumers to attach to the same subscription
                // and get messages dispatched as a queue
                .subscriptionType(SubscriptionType.Key_Shared)
                .subscriptionName(subscription)
                .receiverQueueSize(1)
                .messageListener(listener)
                .subscribe();

    }

    public void subscribe() {

    }

    @SneakyThrows
    @PreDestroy
    public void cleanUp() {
        if (null != consumer && consumer.isConnected()) {
            consumer.close();
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
