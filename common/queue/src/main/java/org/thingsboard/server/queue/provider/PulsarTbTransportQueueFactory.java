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

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;
import org.thingsboard.server.gen.transport.TransportProtos.*;
import org.thingsboard.server.queue.TbQueueAdmin;
import org.thingsboard.server.queue.TbQueueConsumer;
import org.thingsboard.server.queue.TbQueueProducer;
import org.thingsboard.server.queue.TbQueueRequestTemplate;
import org.thingsboard.server.queue.common.DefaultTbQueueRequestTemplate;
import org.thingsboard.server.queue.common.TbProtoQueueMsg;
import org.thingsboard.server.queue.discovery.TbServiceInfoProvider;
import org.thingsboard.server.queue.discovery.TopicService;
import org.thingsboard.server.queue.pulsar.*;
import org.thingsboard.server.queue.settings.TbQueueCoreSettings;
import org.thingsboard.server.queue.settings.TbQueueRuleEngineSettings;
import org.thingsboard.server.queue.settings.TbQueueTransportApiSettings;
import org.thingsboard.server.queue.settings.TbQueueTransportNotificationSettings;

import jakarta.annotation.PreDestroy;

@Component
@ConditionalOnExpression("'${queue.type:null}'=='pulsar' && (('${service.type:null}'=='monolith' && '${transport.api_enabled:true}'=='true') || '${service.type:null}'=='tb-transport')")
@Slf4j
public class PulsarTbTransportQueueFactory implements TbTransportQueueFactory {

    private final TopicService topicService;
    private final TbPulsarSettings settings;
    private final TbServiceInfoProvider serviceInfoProvider;
    private final TbQueueCoreSettings coreSettings;
    private final TbQueueRuleEngineSettings ruleEngineSettings;
    private final TbQueueTransportApiSettings transportApiSettings;
    private final TbQueueTransportNotificationSettings transportNotificationSettings;

    private final TbQueueAdmin coreAdmin;
    private final TbQueueAdmin ruleEngineAdmin;
    private final TbQueueAdmin transportApiRequestAdmin;
    private final TbQueueAdmin transportApiResponseAdmin;
    private final TbQueueAdmin notificationAdmin;

    private final PulsarClient pulsarClient;
    private final PulsarAdmin pulsarAdmin;

    public PulsarTbTransportQueueFactory(TopicService topicService,
                                         TbPulsarSettings settings,
                                         TbServiceInfoProvider serviceInfoProvider,
                                         TbQueueCoreSettings coreSettings,
                                         TbQueueRuleEngineSettings ruleEngineSettings,
                                         TbQueueTransportApiSettings transportApiSettings,
                                         TbQueueTransportNotificationSettings transportNotificationSettings,
                                         TbPulsarTopicConfigs pulsarTopicConfigs,
                                         PulsarClient pulsarClient,
                                         PulsarAdmin pulsarAdmin) {
        this.topicService = topicService;
        this.settings = settings;
        this.serviceInfoProvider = serviceInfoProvider;
        this.coreSettings = coreSettings;
        this.ruleEngineSettings = ruleEngineSettings;
        this.transportApiSettings = transportApiSettings;
        this.transportNotificationSettings = transportNotificationSettings;

        this.pulsarClient = pulsarClient;
        this.pulsarAdmin = pulsarAdmin;

        this.coreAdmin = new TbPulsarAdmin(settings, pulsarTopicConfigs.getCoreConfigs(), this.pulsarAdmin);
        this.ruleEngineAdmin = new TbPulsarAdmin(settings, pulsarTopicConfigs.getRuleEngineConfigs(), this.pulsarAdmin);
        this.transportApiRequestAdmin = new TbPulsarAdmin(settings, pulsarTopicConfigs.getTransportApiRequestConfigs(), this.pulsarAdmin);
        this.transportApiResponseAdmin = new TbPulsarAdmin(settings, pulsarTopicConfigs.getTransportApiResponseConfigs(), this.pulsarAdmin);
        this.notificationAdmin = new TbPulsarAdmin(settings, pulsarTopicConfigs.getNotificationsConfigs(), this.pulsarAdmin);
    }

    @Override
    public TbQueueRequestTemplate<TbProtoQueueMsg<TransportApiRequestMsg>, TbProtoQueueMsg<TransportApiResponseMsg>> createTransportApiRequestTemplate() {
        TbPulsarProducerTemplate.TbPulsarProducerTemplateBuilder<TbProtoQueueMsg<TransportApiRequestMsg>> requestBuilder = TbPulsarProducerTemplate.builder();
        requestBuilder.settings(settings);
        requestBuilder.clientId("transport-api-request-" + serviceInfoProvider.getServiceId());
        requestBuilder.defaultTopic(transportApiSettings.getRequestsTopic());
        requestBuilder.admin(transportApiRequestAdmin);
        requestBuilder.pulsarClient(pulsarClient);

        TbPulsarConsumerTemplate.TbPulsarConsumerTemplateBuilder<TbProtoQueueMsg<TransportApiResponseMsg>> responseBuilder = TbPulsarConsumerTemplate.builder();
        responseBuilder.settings(settings);
        responseBuilder.topic(transportApiSettings.getResponsesTopic() + "." + serviceInfoProvider.getServiceId());
        responseBuilder.clientId("transport-api-response-" + serviceInfoProvider.getServiceId());
        responseBuilder.groupId("transport-node-" + serviceInfoProvider.getServiceId());
        responseBuilder.decoder(msg -> new TbProtoQueueMsg<>(msg.getKey(), TransportApiResponseMsg.parseFrom(msg.getData()), msg.getHeaders()));
        responseBuilder.admin(transportApiResponseAdmin);
        responseBuilder.pulsarClient(pulsarClient);

        DefaultTbQueueRequestTemplate.DefaultTbQueueRequestTemplateBuilder
                <TbProtoQueueMsg<TransportApiRequestMsg>, TbProtoQueueMsg<TransportApiResponseMsg>> templateBuilder = DefaultTbQueueRequestTemplate.builder();
        templateBuilder.queueAdmin(transportApiResponseAdmin);
        templateBuilder.requestTemplate(requestBuilder.build());
        templateBuilder.responseTemplate(responseBuilder.build());
        templateBuilder.maxPendingRequests(transportApiSettings.getMaxPendingRequests());
        templateBuilder.maxRequestTimeout(transportApiSettings.getMaxRequestsTimeout());
        templateBuilder.pollInterval(transportApiSettings.getResponsePollInterval());
        return templateBuilder.build();
    }

    @Override
    public TbQueueProducer<TbProtoQueueMsg<ToRuleEngineMsg>> createRuleEngineMsgProducer() {
        TbPulsarProducerTemplate.TbPulsarProducerTemplateBuilder<TbProtoQueueMsg<ToRuleEngineMsg>> requestBuilder = TbPulsarProducerTemplate.builder();
        requestBuilder.settings(settings);
        requestBuilder.clientId("transport-node-rule-engine-" + serviceInfoProvider.getServiceId());
        requestBuilder.defaultTopic(ruleEngineSettings.getTopic());
        requestBuilder.admin(ruleEngineAdmin);
        requestBuilder.pulsarClient(pulsarClient);
        return requestBuilder.build();
    }

    @Override
    public TbQueueProducer<TbProtoQueueMsg<ToCoreMsg>> createTbCoreMsgProducer() {
        TbPulsarProducerTemplate.TbPulsarProducerTemplateBuilder<TbProtoQueueMsg<ToCoreMsg>> requestBuilder = TbPulsarProducerTemplate.builder();
        requestBuilder.settings(settings);
        requestBuilder.clientId("transport-node-core-" + serviceInfoProvider.getServiceId());
        requestBuilder.defaultTopic(coreSettings.getTopic());
        requestBuilder.admin(coreAdmin);
        requestBuilder.pulsarClient(pulsarClient);
        return requestBuilder.build();
    }

    @Override
    public TbQueueProducer<TbProtoQueueMsg<ToCoreNotificationMsg>> createTbCoreNotificationsMsgProducer() {
        TbPulsarProducerTemplate.TbPulsarProducerTemplateBuilder<TbProtoQueueMsg<ToCoreNotificationMsg>> requestBuilder = TbPulsarProducerTemplate.builder();
        requestBuilder.settings(settings);
        requestBuilder.clientId("transport-node-to-core-notifications-" + serviceInfoProvider.getServiceId());
        requestBuilder.defaultTopic(topicService.buildTopicName(coreSettings.getTopic()));
        requestBuilder.admin(notificationAdmin);
        return requestBuilder.build();
    }

    @Override
    public TbQueueConsumer<TbProtoQueueMsg<ToTransportMsg>> createTransportNotificationsConsumer() {
        TbPulsarConsumerTemplate.TbPulsarConsumerTemplateBuilder<TbProtoQueueMsg<ToTransportMsg>> responseBuilder = TbPulsarConsumerTemplate.builder();
        responseBuilder.settings(settings);
        responseBuilder.topic(transportNotificationSettings.getNotificationsTopic() + "." + serviceInfoProvider.getServiceId());
        responseBuilder.clientId("transport-api-notifications-" + serviceInfoProvider.getServiceId());
        responseBuilder.groupId("transport-node-" + serviceInfoProvider.getServiceId());
        responseBuilder.decoder(msg -> new TbProtoQueueMsg<>(msg.getKey(), ToTransportMsg.parseFrom(msg.getData()), msg.getHeaders()));
        responseBuilder.admin(notificationAdmin);
        responseBuilder.pulsarClient(pulsarClient);
        return responseBuilder.build();
    }

    @Override
    public TbQueueProducer<TbProtoQueueMsg<ToUsageStatsServiceMsg>> createToUsageStatsServiceMsgProducer() {
        TbPulsarProducerTemplate.TbPulsarProducerTemplateBuilder<TbProtoQueueMsg<ToUsageStatsServiceMsg>> requestBuilder = TbPulsarProducerTemplate.builder();
        requestBuilder.settings(settings);
        requestBuilder.clientId("transport-node-us-producer-" + serviceInfoProvider.getServiceId());
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
        if (ruleEngineAdmin != null) {
            ruleEngineAdmin.destroy();
        }
        if (transportApiRequestAdmin != null) {
            transportApiRequestAdmin.destroy();
        }
        if (transportApiResponseAdmin != null) {
            transportApiResponseAdmin.destroy();
        }
        if (notificationAdmin != null) {
            notificationAdmin.destroy();
        }
    }
}
