package org.thingsboard.server.queue.pulsar.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "pulsar.config", ignoreUnknownFields = false)
public class PulsarConfig {

    private String rpcDeviceRequestTopic;
    private String rpcDeviceResponseTopic;
    private String rpcDeviceResponseSubscription;
    private String mqttDevicePublishTopic;
    private String mqttDevicePublishSubscription;
    private int mqttDevicePublishPartition;
    private String mqttDeviceSubscribeTopic;
    private String mqttDeviceSubscribeSubscription;
    private String mqttClientEventTopic;
    private String mqttClientEventSubscription;
    private int mqttClientEventPartition;

//    @Bean
//    public PulsarListenerConsumerBuilderCustomizer<String> devicePublishConsumerConfig() {
//        return cb -> {
//            cb.subscriptionName(mqttDevicePublishSubscription);
//            cb.topic(mqttDevicePublishTopic);
//        };
//    }

//    @Bean
//    public SchemaResolver.SchemaResolverCustomizer<DefaultSchemaResolver> schemaResolverCustomizer() {
//        return (schemaResolver) -> {
//            schemaResolver.addCustomSchemaMapping(MqttMessage.class, Schema.JSON(MqttMessage.class));
//        };
//    }
}
