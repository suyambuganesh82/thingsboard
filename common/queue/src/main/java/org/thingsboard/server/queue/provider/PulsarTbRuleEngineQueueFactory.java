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
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import org.thingsboard.server.common.data.queue.Queue;
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
import org.thingsboard.server.queue.settings.TbQueueCoreSettings;
import org.thingsboard.server.queue.settings.TbQueueRemoteJsInvokeSettings;
import org.thingsboard.server.queue.settings.TbQueueRuleEngineSettings;
import org.thingsboard.server.queue.settings.TbQueueTransportNotificationSettings;

import jakarta.annotation.PreDestroy;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

@Component
@ConditionalOnExpression("'${queue.type:null}'=='pulsar' && '${service.type:null}'=='tb-rule-engine'")
public class PulsarTbRuleEngineQueueFactory implements TbRuleEngineQueueFactory {

    private final TopicService topicService;
    private final TbPulsarSettings settings;
    private final TbServiceInfoProvider serviceInfoProvider;
    private final TbQueueCoreSettings coreSettings;
    private final TbQueueRuleEngineSettings ruleEngineSettings;
    private final TbQueueRemoteJsInvokeSettings jsInvokeSettings;
    private final TbQueueTransportNotificationSettings transportNotificationSettings;

    private final PulsarClient pulsarClient;
    private final PulsarAdmin pulsarAdmin;

    private final TbQueueAdmin coreAdmin;
    private final TbQueueAdmin ruleEngineAdmin;
    private final TbQueueAdmin jsExecutorRequestAdmin;
    private final TbQueueAdmin jsExecutorResponseAdmin;
    private final TbQueueAdmin notificationAdmin;
    private final TbQueueAdmin fwUpdatesAdmin;
    private final AtomicLong consumerCount = new AtomicLong();

    public PulsarTbRuleEngineQueueFactory(TopicService topicService, TbPulsarSettings settings,
                                          TbServiceInfoProvider serviceInfoProvider,
                                          TbQueueCoreSettings coreSettings,
                                          TbQueueRuleEngineSettings ruleEngineSettings,
                                          TbQueueRemoteJsInvokeSettings jsInvokeSettings,
                                          TbQueueTransportNotificationSettings transportNotificationSettings,
                                          TbPulsarTopicConfigs pulsarTopicConfigs,
                                          PulsarClient pulsarClient,
                                          PulsarAdmin pulsarAdmin) {
        this.topicService = topicService;
        this.settings = settings;
        this.serviceInfoProvider = serviceInfoProvider;
        this.coreSettings = coreSettings;
        this.ruleEngineSettings = ruleEngineSettings;
        this.jsInvokeSettings = jsInvokeSettings;
        this.transportNotificationSettings = transportNotificationSettings;

        this.pulsarClient = pulsarClient;
        this.pulsarAdmin = pulsarAdmin;

        this.coreAdmin = new TbPulsarAdmin(settings, pulsarTopicConfigs.getCoreConfigs(), this.pulsarAdmin);
        this.ruleEngineAdmin = new TbPulsarAdmin(settings, pulsarTopicConfigs.getRuleEngineConfigs(), this.pulsarAdmin);
        this.jsExecutorRequestAdmin = new TbPulsarAdmin(settings, pulsarTopicConfigs.getJsExecutorRequestConfigs(), this.pulsarAdmin);
        this.jsExecutorResponseAdmin = new TbPulsarAdmin(settings, pulsarTopicConfigs.getJsExecutorResponseConfigs(), this.pulsarAdmin);
        this.notificationAdmin = new TbPulsarAdmin(settings, pulsarTopicConfigs.getNotificationsConfigs(), this.pulsarAdmin);
        this.fwUpdatesAdmin = new TbPulsarAdmin(settings, pulsarTopicConfigs.getFwUpdatesConfigs(), this.pulsarAdmin);
    }

    @Override
    public TbQueueProducer<TbProtoQueueMsg<ToTransportMsg>> createTransportNotificationsMsgProducer() {
        TbPulsarProducerTemplate.TbPulsarProducerTemplateBuilder<TbProtoQueueMsg<ToTransportMsg>> requestBuilder = TbPulsarProducerTemplate.builder();
        requestBuilder.settings(settings);
        requestBuilder.clientId("tb-rule-engine-transport-notifications-" + serviceInfoProvider.getServiceId());
        requestBuilder.defaultTopic(transportNotificationSettings.getNotificationsTopic());
        requestBuilder.admin(notificationAdmin);
        requestBuilder.pulsarClient(pulsarClient);
        return requestBuilder.build();
    }

    @Override
    public TbQueueProducer<TbProtoQueueMsg<ToRuleEngineMsg>> createRuleEngineMsgProducer() {
        TbPulsarProducerTemplate.TbPulsarProducerTemplateBuilder<TbProtoQueueMsg<ToRuleEngineMsg>> requestBuilder = TbPulsarProducerTemplate.builder();
        requestBuilder.settings(settings);
        requestBuilder.clientId("tb-rule-engine-to-rule-engine-" + serviceInfoProvider.getServiceId());
        requestBuilder.defaultTopic(ruleEngineSettings.getTopic());
        requestBuilder.admin(ruleEngineAdmin);
        requestBuilder.pulsarClient(pulsarClient);
        return requestBuilder.build();
    }

    @Override
    public TbQueueProducer<TbProtoQueueMsg<ToRuleEngineNotificationMsg>> createRuleEngineNotificationsMsgProducer() {
        TbPulsarProducerTemplate.TbPulsarProducerTemplateBuilder<TbProtoQueueMsg<ToRuleEngineNotificationMsg>> requestBuilder = TbPulsarProducerTemplate.builder();
        requestBuilder.settings(settings);
        requestBuilder.clientId("tb-rule-engine-to-rule-engine-notifications-" + serviceInfoProvider.getServiceId());
        requestBuilder.defaultTopic(ruleEngineSettings.getTopic());
        requestBuilder.admin(notificationAdmin);
        requestBuilder.pulsarClient(pulsarClient);
        return requestBuilder.build();
    }

    @Override
    public TbQueueProducer<TbProtoQueueMsg<ToCoreMsg>> createTbCoreMsgProducer() {
        TbPulsarProducerTemplate.TbPulsarProducerTemplateBuilder<TbProtoQueueMsg<ToCoreMsg>> requestBuilder = TbPulsarProducerTemplate.builder();
        requestBuilder.settings(settings);
        requestBuilder.clientId("tb-rule-engine-to-core-" + serviceInfoProvider.getServiceId());
        requestBuilder.defaultTopic(coreSettings.getTopic());
        requestBuilder.admin(coreAdmin);
        requestBuilder.pulsarClient(pulsarClient);
        return requestBuilder.build();
    }

    @Override
    public TbQueueProducer<TbProtoQueueMsg<ToOtaPackageStateServiceMsg>> createToOtaPackageStateServiceMsgProducer() {
        TbPulsarProducerTemplate.TbPulsarProducerTemplateBuilder<TbProtoQueueMsg<ToOtaPackageStateServiceMsg>> requestBuilder = TbPulsarProducerTemplate.builder();
        requestBuilder.settings(settings);
        requestBuilder.clientId("tb-rule-engine-ota-producer-" + serviceInfoProvider.getServiceId());
        requestBuilder.defaultTopic(coreSettings.getOtaPackageTopic());
        requestBuilder.admin(fwUpdatesAdmin);
        requestBuilder.pulsarClient(pulsarClient);
        return requestBuilder.build();
    }

    @Override
    public TbQueueProducer<TbProtoQueueMsg<ToCoreNotificationMsg>> createTbCoreNotificationsMsgProducer() {
        TbPulsarProducerTemplate.TbPulsarProducerTemplateBuilder<TbProtoQueueMsg<ToCoreNotificationMsg>> requestBuilder = TbPulsarProducerTemplate.builder();
        requestBuilder.settings(settings);
        requestBuilder.clientId("tb-rule-engine-to-core-notifications-" + serviceInfoProvider.getServiceId());
        requestBuilder.defaultTopic(coreSettings.getTopic());
        requestBuilder.admin(notificationAdmin);
        requestBuilder.pulsarClient(pulsarClient);
        return requestBuilder.build();
    }

    @Override
    public TbQueueConsumer<TbProtoQueueMsg<ToRuleEngineMsg>> createToRuleEngineMsgConsumer(Queue configuration) {
        String queueName = configuration.getName();
        TbPulsarConsumerTemplate.TbPulsarConsumerTemplateBuilder<TbProtoQueueMsg<ToRuleEngineMsg>> consumerBuilder = TbPulsarConsumerTemplate.builder();
        consumerBuilder.settings(settings);
        consumerBuilder.topic(configuration.getTopic());
        consumerBuilder.clientId("re-" + queueName + "-consumer-" + serviceInfoProvider.getServiceId() + "-" + consumerCount.incrementAndGet());
        consumerBuilder.groupId("re-" + queueName + "-consumer");
        consumerBuilder.decoder(msg -> new TbProtoQueueMsg<>(msg.getKey(), ToRuleEngineMsg.parseFrom(msg.getData()), msg.getHeaders()));
        consumerBuilder.admin(ruleEngineAdmin);
        consumerBuilder.pulsarClient(pulsarClient);
        return consumerBuilder.build();
    }

    @Override
    public TbQueueConsumer<TbProtoQueueMsg<ToRuleEngineNotificationMsg>> createToRuleEngineNotificationsMsgConsumer() {
        TbPulsarConsumerTemplate.TbPulsarConsumerTemplateBuilder<TbProtoQueueMsg<ToRuleEngineNotificationMsg>> consumerBuilder = TbPulsarConsumerTemplate.builder();
        consumerBuilder.settings(settings);
        consumerBuilder.topic(topicService.getNotificationsTopic(ServiceType.TB_RULE_ENGINE, serviceInfoProvider.getServiceId()).getFullTopicName());
        consumerBuilder.clientId("tb-rule-engine-notifications-consumer-" + serviceInfoProvider.getServiceId());
        consumerBuilder.groupId("tb-rule-engine-notifications-node-" + serviceInfoProvider.getServiceId());
        consumerBuilder.decoder(msg -> new TbProtoQueueMsg<>(msg.getKey(), ToRuleEngineNotificationMsg.parseFrom(msg.getData()), msg.getHeaders()));
        consumerBuilder.admin(notificationAdmin);
        consumerBuilder.pulsarClient(pulsarClient);
        return consumerBuilder.build();
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
    public TbQueueProducer<TbProtoQueueMsg<ToUsageStatsServiceMsg>> createToUsageStatsServiceMsgProducer() {
        TbPulsarProducerTemplate.TbPulsarProducerTemplateBuilder<TbProtoQueueMsg<ToUsageStatsServiceMsg>> requestBuilder = TbPulsarProducerTemplate.builder();
        requestBuilder.settings(settings);
        requestBuilder.clientId("tb-rule-engine-us-producer-" + serviceInfoProvider.getServiceId());
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
        if (jsExecutorRequestAdmin != null) {
            jsExecutorRequestAdmin.destroy();
        }
        if (jsExecutorResponseAdmin != null) {
            jsExecutorResponseAdmin.destroy();
        }
        if (notificationAdmin != null) {
            notificationAdmin.destroy();
        }
        if (fwUpdatesAdmin != null) {
            fwUpdatesAdmin.destroy();
        }
    }
}
