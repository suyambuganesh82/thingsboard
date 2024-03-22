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
package org.thingsboard.server.queue.pulsar;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClientException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Slf4j
@ConditionalOnProperty(prefix = "queue", value = "type", havingValue = "pulsar")
@ConfigurationProperties(prefix = "queue.pulsar")
@Component
public class TbPulsarSettings {

    boolean useTls = false;
    boolean tlsAllowInsecureConnection = false;
    String tlsTrustCertsFilePath = null;
    @Value("${queue.pulsar.bootstrap.servers}")
    private String broker;
    @Value("${queue.pulsar.admin.servers}")
    private String admin;
    @Value("${queue.pulsar.cluster}")
    private String cluster;

    public String getBroker() {
        return broker;
    }

    public String getAdmin() {
        return admin;
    }

//    @Bean
//    public PulsarClient pulsarClient() throws PulsarClientException {
//        return PulsarClient.builder()
//                .serviceUrl(broker)
//                .build();
//    }

    @Bean
    public PulsarAdmin pulsarAdmin() throws PulsarClientException {
        return PulsarAdmin.builder()
//                .authentication(authPluginClassName, authParams)
                .serviceHttpUrl(admin)
                .tlsTrustCertsFilePath(tlsTrustCertsFilePath)
                .allowTlsInsecureConnection(tlsAllowInsecureConnection)
                .build();
    }

    public String getCluster() {
        return cluster;
    }
}
