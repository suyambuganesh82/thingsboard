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
