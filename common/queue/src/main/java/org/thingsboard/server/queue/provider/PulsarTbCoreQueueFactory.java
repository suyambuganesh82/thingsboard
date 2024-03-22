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

import com.google.protobuf.util.JsonFormat;
import jakarta.annotation.PreDestroy;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import org.thingsboard.server.common.msg.queue.ServiceType;
import org.thingsboard.server.gen.js.JsInvokeProtos;
import org.thingsboard.server.gen.transport.TransportProtos.*;
import org.thingsboard.server.queue.TbQueueAdmin;
import org.thingsboard.server.queue.TbQueueConsumer;
import org.thingsboard.server.queue.TbQueueProducer;
import org.thingsboard.server.queue.TbQueueRequestTemplate;
import org.thingsboard.server.queue.common.DefaultTbQueueRequestTemplate;
import org.thingsboard.server.queue.common.TbProtoJsQueueMsg;
import org.thingsboard.server.queue.common.TbProtoQueueMsg;
import org.thingsboard.server.queue.discovery.TbServiceInfoProvider;
import org.thingsboard.server.queue.discovery.TopicService;
import org.thingsboard.server.queue.pulsar.*;
import org.thingsboard.server.queue.settings.*;

import java.nio.charset.StandardCharsets;

@Component
@ConditionalOnExpression("'${queue.type:null}'=='pulsar' && '${service.type:null}'=='tb-core'")
public class PulsarTbCoreQueueFactory implements TbCoreQueueFactory {

    private final TopicService topicService;
    private final TbPulsarSettings settings;
    private final TbServiceInfoProvider serviceInfoProvider;
    private final TbQueueCoreSettings coreSettings;
    private final TbQueueRuleEngineSettings ruleEngineSettings;
    private final TbQueueTransportApiSettings transportApiSettings;
    private final TbQueueRemoteJsInvokeSettings jsInvokeSettings;
    private final TbQueueVersionControlSettings vcSettings;
    private final TbQueueTransportNotificationSettings transportNotificationSettings;

    private final PulsarClient pulsarClient;
    private final PulsarAdmin pulsarAdmin;

    private final TbQueueAdmin coreAdmin;
    private final TbQueueAdmin ruleEngineAdmin;
    private final TbQueueAdmin jsExecutorRequestAdmin;
    private final TbQueueAdmin jsExecutorResponseAdmin;
    private final TbQueueAdmin transportApiRequestAdmin;
    private final TbQueueAdmin transportApiResponseAdmin;
    private final TbQueueAdmin notificationAdmin;
    private final TbQueueAdmin fwUpdatesAdmin;
    private final TbQueueAdmin vcAdmin;

    public PulsarTbCoreQueueFactory(TopicService topicService,
                                    TbPulsarSettings settings,
                                    TbServiceInfoProvider serviceInfoProvider,
                                    TbQueueCoreSettings coreSettings,
                                    TbQueueRuleEngineSettings ruleEngineSettings,
                                    TbQueueTransportApiSettings transportApiSettings,
                                    TbQueueRemoteJsInvokeSettings jsInvokeSettings,
                                    TbQueueVersionControlSettings vcSettings,
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
        this.jsInvokeSettings = jsInvokeSettings;
        this.vcSettings = vcSettings;
        this.transportNotificationSettings = transportNotificationSettings;

        this.pulsarClient = pulsarClient;
        this.pulsarAdmin = pulsarAdmin;

        this.coreAdmin = new TbPulsarAdmin(settings, pulsarTopicConfigs.getCoreConfigs(), this.pulsarAdmin);
        this.ruleEngineAdmin = new TbPulsarAdmin(settings, pulsarTopicConfigs.getRuleEngineConfigs(), this.pulsarAdmin);
        this.jsExecutorRequestAdmin = new TbPulsarAdmin(settings, pulsarTopicConfigs.getJsExecutorRequestConfigs(), this.pulsarAdmin);
        this.jsExecutorResponseAdmin = new TbPulsarAdmin(settings, pulsarTopicConfigs.getJsExecutorResponseConfigs(), this.pulsarAdmin);
        this.transportApiRequestAdmin = new TbPulsarAdmin(settings, pulsarTopicConfigs.getTransportApiRequestConfigs(), this.pulsarAdmin);
        this.transportApiResponseAdmin = new TbPulsarAdmin(settings, pulsarTopicConfigs.getTransportApiResponseConfigs(), this.pulsarAdmin);
        this.notificationAdmin = new TbPulsarAdmin(settings, pulsarTopicConfigs.getNotificationsConfigs(), this.pulsarAdmin);
        this.fwUpdatesAdmin = new TbPulsarAdmin(settings, pulsarTopicConfigs.getFwUpdatesConfigs(), this.pulsarAdmin);
        this.vcAdmin = new TbPulsarAdmin(settings, pulsarTopicConfigs.getVcConfigs(), this.pulsarAdmin);
    }

    @Override
    public TbQueueProducer<TbProtoQueueMsg<ToTransportMsg>> createTransportNotificationsMsgProducer() {
        TbPulsarProducerTemplate.TbPulsarProducerTemplateBuilder<TbProtoQueueMsg<ToTransportMsg>> requestBuilder = TbPulsarProducerTemplate.builder();
        requestBuilder.settings(settings);
        requestBuilder.clientId("tb-core-transport-notifications-" + serviceInfoProvider.getServiceId());
        requestBuilder.defaultTopic(transportNotificationSettings.getNotificationsTopic());
        requestBuilder.admin(notificationAdmin);
        requestBuilder.pulsarClient(pulsarClient);
        return requestBuilder.build();
    }

    @Override
    public TbQueueProducer<TbProtoQueueMsg<ToRuleEngineMsg>> createRuleEngineMsgProducer() {
        TbPulsarProducerTemplate.TbPulsarProducerTemplateBuilder<TbProtoQueueMsg<ToRuleEngineMsg>> requestBuilder = TbPulsarProducerTemplate.builder();
        requestBuilder.settings(settings);
        requestBuilder.clientId("tb-core-rule-engine-" + serviceInfoProvider.getServiceId());
        requestBuilder.defaultTopic(coreSettings.getTopic());
        requestBuilder.admin(coreAdmin);
        requestBuilder.pulsarClient(pulsarClient);
        return requestBuilder.build();
    }

    @Override
    public TbQueueProducer<TbProtoQueueMsg<ToRuleEngineNotificationMsg>> createRuleEngineNotificationsMsgProducer() {
        TbPulsarProducerTemplate.TbPulsarProducerTemplateBuilder<TbProtoQueueMsg<ToRuleEngineNotificationMsg>> requestBuilder = TbPulsarProducerTemplate.builder();
        requestBuilder.settings(settings);
        requestBuilder.clientId("tb-core-rule-engine-notifications-" + serviceInfoProvider.getServiceId());
        requestBuilder.defaultTopic(ruleEngineSettings.getTopic());
        requestBuilder.admin(notificationAdmin);
        requestBuilder.pulsarClient(pulsarClient);
        return requestBuilder.build();
    }

    @Override
    public TbQueueProducer<TbProtoQueueMsg<ToCoreMsg>> createTbCoreMsgProducer() {
        TbPulsarProducerTemplate.TbPulsarProducerTemplateBuilder<TbProtoQueueMsg<ToCoreMsg>> requestBuilder = TbPulsarProducerTemplate.builder();
        requestBuilder.settings(settings);
        requestBuilder.clientId("tb-core-to-core-" + serviceInfoProvider.getServiceId());
        requestBuilder.defaultTopic(coreSettings.getTopic());
        requestBuilder.admin(coreAdmin);
        requestBuilder.pulsarClient(pulsarClient);
        return requestBuilder.build();
    }

    @Override
    public TbQueueProducer<TbProtoQueueMsg<ToCoreNotificationMsg>> createTbCoreNotificationsMsgProducer() {
        TbPulsarProducerTemplate.TbPulsarProducerTemplateBuilder<TbProtoQueueMsg<ToCoreNotificationMsg>> requestBuilder = TbPulsarProducerTemplate.builder();
        requestBuilder.settings(settings);
        requestBuilder.clientId("tb-core-to-core-notifications-" + serviceInfoProvider.getServiceId());
        requestBuilder.defaultTopic(coreSettings.getTopic());
        requestBuilder.admin(notificationAdmin);
        requestBuilder.pulsarClient(pulsarClient);
        return requestBuilder.build();
    }

    @Override
    public TbQueueConsumer<TbProtoQueueMsg<ToCoreMsg>> createToCoreMsgConsumer() {
        TbPulsarConsumerTemplate.TbPulsarConsumerTemplateBuilder<TbProtoQueueMsg<ToCoreMsg>> consumerBuilder = TbPulsarConsumerTemplate.builder();
        consumerBuilder.settings(settings);
        consumerBuilder.topic(coreSettings.getTopic());
        consumerBuilder.clientId("tb-core-consumer-" + serviceInfoProvider.getServiceId());
        consumerBuilder.groupId("tb-core-node");
        consumerBuilder.decoder(msg -> new TbProtoQueueMsg<>(msg.getKey(), ToCoreMsg.parseFrom(msg.getData()), msg.getHeaders()));
        consumerBuilder.admin(coreAdmin);
        consumerBuilder.pulsarClient(pulsarClient);
        return consumerBuilder.build();
    }

    @Override
    public TbQueueConsumer<TbProtoQueueMsg<ToCoreNotificationMsg>> createToCoreNotificationsMsgConsumer() {
        TbPulsarConsumerTemplate.TbPulsarConsumerTemplateBuilder<TbProtoQueueMsg<ToCoreNotificationMsg>> consumerBuilder = TbPulsarConsumerTemplate.builder();
        consumerBuilder.settings(settings);
        consumerBuilder.topic(topicService.getNotificationsTopic(ServiceType.TB_CORE, serviceInfoProvider.getServiceId()).getFullTopicName());
        consumerBuilder.clientId("tb-core-notifications-consumer-" + serviceInfoProvider.getServiceId());
        consumerBuilder.groupId("tb-core-notifications-node-" + serviceInfoProvider.getServiceId());
        consumerBuilder.decoder(msg -> new TbProtoQueueMsg<>(msg.getKey(), ToCoreNotificationMsg.parseFrom(msg.getData()), msg.getHeaders()));
        consumerBuilder.admin(notificationAdmin);
        consumerBuilder.pulsarClient(pulsarClient);
        return consumerBuilder.build();
    }

    @Override
    public TbQueueConsumer<TbProtoQueueMsg<TransportApiRequestMsg>> createTransportApiRequestConsumer() {
        TbPulsarConsumerTemplate.TbPulsarConsumerTemplateBuilder<TbProtoQueueMsg<TransportApiRequestMsg>> consumerBuilder = TbPulsarConsumerTemplate.builder();
        consumerBuilder.settings(settings);
        consumerBuilder.topic(transportApiSettings.getRequestsTopic());
        consumerBuilder.clientId("tb-core-transport-api-consumer-" + serviceInfoProvider.getServiceId());
        consumerBuilder.groupId("tb-core-transport-api-consumer");
        consumerBuilder.decoder(msg -> new TbProtoQueueMsg<>(msg.getKey(), TransportApiRequestMsg.parseFrom(msg.getData()), msg.getHeaders()));
        consumerBuilder.admin(transportApiRequestAdmin);
        consumerBuilder.pulsarClient(pulsarClient);
        return consumerBuilder.build();
    }

    @Override
    public TbQueueProducer<TbProtoQueueMsg<TransportApiResponseMsg>> createTransportApiResponseProducer() {
        TbPulsarProducerTemplate.TbPulsarProducerTemplateBuilder<TbProtoQueueMsg<TransportApiResponseMsg>> requestBuilder = TbPulsarProducerTemplate.builder();
        requestBuilder.settings(settings);
        requestBuilder.clientId("tb-core-transport-api-producer-" + serviceInfoProvider.getServiceId());
        requestBuilder.defaultTopic(transportApiSettings.getResponsesTopic());
        requestBuilder.admin(transportApiResponseAdmin);
        requestBuilder.pulsarClient(pulsarClient);
        return requestBuilder.build();
    }

    @Override
    @Bean
    public TbQueueRequestTemplate<TbProtoJsQueueMsg<JsInvokeProtos.RemoteJsRequest>, TbProtoQueueMsg<JsInvokeProtos.RemoteJsResponse>> createRemoteJsRequestTemplate() {
        TbPulsarProducerTemplate.TbPulsarProducerTemplateBuilder<TbProtoJsQueueMsg<JsInvokeProtos.RemoteJsRequest>> requestBuilder = TbPulsarProducerTemplate.builder();
        requestBuilder.settings(settings);
        requestBuilder.clientId("producer-js-invoke-" + serviceInfoProvider.getServiceId());
        requestBuilder.defaultTopic(jsInvokeSettings.getRequestTopic());
        requestBuilder.admin(jsExecutorRequestAdmin);
        requestBuilder.pulsarClient(pulsarClient);

        TbPulsarConsumerTemplate.TbPulsarConsumerTemplateBuilder<TbProtoQueueMsg<JsInvokeProtos.RemoteJsResponse>> responseBuilder = TbPulsarConsumerTemplate.builder();
        responseBuilder.settings(settings);
        responseBuilder.topic(jsInvokeSettings.getResponseTopic() + "." + serviceInfoProvider.getServiceId());
        responseBuilder.clientId("js-" + serviceInfoProvider.getServiceId());
        responseBuilder.groupId("rule-engine-node-" + serviceInfoProvider.getServiceId());
        responseBuilder.decoder(msg -> {
                    JsInvokeProtos.RemoteJsResponse.Builder builder = JsInvokeProtos.RemoteJsResponse.newBuilder();
                    JsonFormat.parser().ignoringUnknownFields().merge(new String(msg.getData(), StandardCharsets.UTF_8), builder);
                    return new TbProtoQueueMsg<>(msg.getKey(), builder.build(), msg.getHeaders());
                }
        );
        responseBuilder.admin(jsExecutorResponseAdmin);
        responseBuilder.pulsarClient(pulsarClient);

        DefaultTbQueueRequestTemplate.DefaultTbQueueRequestTemplateBuilder
                <TbProtoJsQueueMsg<JsInvokeProtos.RemoteJsRequest>, TbProtoQueueMsg<JsInvokeProtos.RemoteJsResponse>> builder = DefaultTbQueueRequestTemplate.builder();
        builder.queueAdmin(jsExecutorResponseAdmin);
        builder.requestTemplate(requestBuilder.build());
        builder.responseTemplate(responseBuilder.build());
        builder.maxPendingRequests(jsInvokeSettings.getMaxPendingRequests());
        builder.maxRequestTimeout(jsInvokeSettings.getMaxRequestsTimeout());
        builder.pollInterval(jsInvokeSettings.getResponsePollInterval());
        return builder.build();
    }

    @Override
    public TbQueueConsumer<TbProtoQueueMsg<ToUsageStatsServiceMsg>> createToUsageStatsServiceMsgConsumer() {
        TbPulsarConsumerTemplate.TbPulsarConsumerTemplateBuilder<TbProtoQueueMsg<ToUsageStatsServiceMsg>> consumerBuilder = TbPulsarConsumerTemplate.builder();
        consumerBuilder.settings(settings);
        consumerBuilder.topic(coreSettings.getUsageStatsTopic());
        consumerBuilder.clientId("tb-core-us-consumer-" + serviceInfoProvider.getServiceId());
        consumerBuilder.groupId("tb-core-us-consumer");
        consumerBuilder.decoder(msg -> new TbProtoQueueMsg<>(msg.getKey(), ToUsageStatsServiceMsg.parseFrom(msg.getData()), msg.getHeaders()));
        consumerBuilder.admin(coreAdmin);
        consumerBuilder.pulsarClient(pulsarClient);
        return consumerBuilder.build();
    }

    @Override
    public TbQueueConsumer<TbProtoQueueMsg<ToOtaPackageStateServiceMsg>> createToOtaPackageStateServiceMsgConsumer() {
        TbPulsarConsumerTemplate.TbPulsarConsumerTemplateBuilder<TbProtoQueueMsg<ToOtaPackageStateServiceMsg>> consumerBuilder = TbPulsarConsumerTemplate.builder();
        consumerBuilder.settings(settings);
        consumerBuilder.topic(coreSettings.getOtaPackageTopic());
        consumerBuilder.clientId("tb-core-ota-consumer-" + serviceInfoProvider.getServiceId());
        consumerBuilder.groupId("tb-core-ota-consumer");
        consumerBuilder.decoder(msg -> new TbProtoQueueMsg<>(msg.getKey(), ToOtaPackageStateServiceMsg.parseFrom(msg.getData()), msg.getHeaders()));
        consumerBuilder.admin(fwUpdatesAdmin);
        consumerBuilder.pulsarClient(pulsarClient);
        return consumerBuilder.build();
    }

    @Override
    public TbQueueProducer<TbProtoQueueMsg<ToOtaPackageStateServiceMsg>> createToOtaPackageStateServiceMsgProducer() {
        TbPulsarProducerTemplate.TbPulsarProducerTemplateBuilder<TbProtoQueueMsg<ToOtaPackageStateServiceMsg>> requestBuilder = TbPulsarProducerTemplate.builder();
        requestBuilder.settings(settings);
        requestBuilder.clientId("tb-core-ota-producer-" + serviceInfoProvider.getServiceId());
        requestBuilder.defaultTopic(coreSettings.getOtaPackageTopic());
        requestBuilder.admin(fwUpdatesAdmin);
        requestBuilder.pulsarClient(pulsarClient);
        return requestBuilder.build();
    }

    @Override
    public TbQueueProducer<TbProtoQueueMsg<ToUsageStatsServiceMsg>> createToUsageStatsServiceMsgProducer() {
        TbPulsarProducerTemplate.TbPulsarProducerTemplateBuilder<TbProtoQueueMsg<ToUsageStatsServiceMsg>> requestBuilder = TbPulsarProducerTemplate.builder();
        requestBuilder.settings(settings);
        requestBuilder.clientId("tb-core-us-producer-" + serviceInfoProvider.getServiceId());
        requestBuilder.defaultTopic(coreSettings.getUsageStatsTopic());
        requestBuilder.admin(coreAdmin);
        requestBuilder.pulsarClient(pulsarClient);
        return requestBuilder.build();
    }

    @Override
    public TbQueueProducer<TbProtoQueueMsg<ToVersionControlServiceMsg>> createVersionControlMsgProducer() {
        TbPulsarProducerTemplate.TbPulsarProducerTemplateBuilder<TbProtoQueueMsg<ToVersionControlServiceMsg>> requestBuilder = TbPulsarProducerTemplate.builder();
        requestBuilder.settings(settings);
        requestBuilder.clientId("tb-core-vc-producer-" + serviceInfoProvider.getServiceId());
        requestBuilder.defaultTopic(vcSettings.getTopic());
        requestBuilder.admin(vcAdmin);
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
        if (jsExecutorRequestAdmin != null) {
            jsExecutorRequestAdmin.destroy();
        }
        if (jsExecutorResponseAdmin != null) {
            jsExecutorResponseAdmin.destroy();
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
        if (fwUpdatesAdmin != null) {
            fwUpdatesAdmin.destroy();
        }
        if (vcAdmin != null) {
            vcAdmin.destroy();
        }
    }
}
