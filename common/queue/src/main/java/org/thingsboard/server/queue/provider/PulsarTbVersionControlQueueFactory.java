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
package org.thingsboard.server.queue.provider;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;
import org.thingsboard.server.gen.transport.TransportProtos.ToCoreNotificationMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToUsageStatsServiceMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToVersionControlServiceMsg;
import org.thingsboard.server.queue.TbQueueAdmin;
import org.thingsboard.server.queue.TbQueueConsumer;
import org.thingsboard.server.queue.TbQueueProducer;
import org.thingsboard.server.queue.common.TbProtoQueueMsg;
import org.thingsboard.server.queue.discovery.TbServiceInfoProvider;
import org.thingsboard.server.queue.pulsar.*;
import org.thingsboard.server.queue.settings.TbQueueCoreSettings;
import org.thingsboard.server.queue.settings.TbQueueVersionControlSettings;

import jakarta.annotation.PreDestroy;

@Component
@ConditionalOnExpression("'${queue.type:null}'=='pulsar' && '${service.type:null}'=='tb-vc-executor'")
public class PulsarTbVersionControlQueueFactory implements TbVersionControlQueueFactory {

    private final TbPulsarSettings settings;
    private final TbServiceInfoProvider serviceInfoProvider;
    private final TbQueueCoreSettings coreSettings;
    private final TbQueueVersionControlSettings vcSettings;

    private final PulsarClient pulsarClient;
    private final PulsarAdmin pulsarAdmin;

    private final TbQueueAdmin coreAdmin;
    private final TbQueueAdmin vcAdmin;
    private final TbQueueAdmin notificationAdmin;

    public PulsarTbVersionControlQueueFactory(TbPulsarSettings settings,
                                              TbServiceInfoProvider serviceInfoProvider,
                                              TbQueueCoreSettings coreSettings,
                                              TbQueueVersionControlSettings vcSettings,
                                              TbPulsarTopicConfigs pulsarTopicConfigs,
                                              PulsarClient pulsarClient,
                                              PulsarAdmin pulsarAdmin) {
        this.settings = settings;
        this.serviceInfoProvider = serviceInfoProvider;
        this.coreSettings = coreSettings;
        this.vcSettings = vcSettings;

        this.pulsarClient = pulsarClient;
        this.pulsarAdmin = pulsarAdmin;

        this.coreAdmin = new TbPulsarAdmin(settings, pulsarTopicConfigs.getCoreConfigs(), this.pulsarAdmin);
        this.vcAdmin = new TbPulsarAdmin(settings, pulsarTopicConfigs.getVcConfigs(), this.pulsarAdmin);
        this.notificationAdmin = new TbPulsarAdmin(settings, pulsarTopicConfigs.getNotificationsConfigs(), this.pulsarAdmin);
    }


    @Override
    public TbQueueProducer<TbProtoQueueMsg<ToCoreNotificationMsg>> createTbCoreNotificationsMsgProducer() {
        TbPulsarProducerTemplate.TbPulsarProducerTemplateBuilder<TbProtoQueueMsg<ToCoreNotificationMsg>> requestBuilder = TbPulsarProducerTemplate.builder();
        requestBuilder.settings(settings);
        requestBuilder.clientId("tb-vc-to-core-notifications-" + serviceInfoProvider.getServiceId());
        requestBuilder.defaultTopic(coreSettings.getTopic());
        requestBuilder.admin(notificationAdmin);
        requestBuilder.pulsarClient(pulsarClient);
        return requestBuilder.build();
    }

    @Override
    public TbQueueConsumer<TbProtoQueueMsg<ToVersionControlServiceMsg>> createToVersionControlMsgConsumer() {
        TbPulsarConsumerTemplate.TbPulsarConsumerTemplateBuilder<TbProtoQueueMsg<ToVersionControlServiceMsg>> consumerBuilder = TbPulsarConsumerTemplate.builder();
        consumerBuilder.settings(settings);
        consumerBuilder.topic(vcSettings.getTopic());
        consumerBuilder.clientId("tb-vc-consumer-" + serviceInfoProvider.getServiceId());
        consumerBuilder.groupId("tb-vc-node");
        consumerBuilder.decoder(msg -> new TbProtoQueueMsg<>(msg.getKey(), ToVersionControlServiceMsg.parseFrom(msg.getData()), msg.getHeaders()));
        consumerBuilder.admin(vcAdmin);
        consumerBuilder.pulsarClient(pulsarClient);
        return consumerBuilder.build();
    }

    @Override
    public TbQueueProducer<TbProtoQueueMsg<ToUsageStatsServiceMsg>> createToUsageStatsServiceMsgProducer() {
        TbPulsarProducerTemplate.TbPulsarProducerTemplateBuilder<TbProtoQueueMsg<ToUsageStatsServiceMsg>> requestBuilder = TbPulsarProducerTemplate.builder();
        requestBuilder.settings(settings);
        requestBuilder.clientId("tb-vc-us-producer-" + serviceInfoProvider.getServiceId());
        requestBuilder.defaultTopic(coreSettings.getUsageStatsTopic());
        requestBuilder.admin(coreAdmin);
        requestBuilder.pulsarClient(pulsarClient);
        return requestBuilder.build();
    }

    @PreDestroy
    private void destroy() {
        if (coreAdmin != null) {
            coreAdmin.destroy();
        }
        if (vcAdmin != null) {
            vcAdmin.destroy();
        }
        if (notificationAdmin != null) {
            notificationAdmin.destroy();
        }
    }
}
