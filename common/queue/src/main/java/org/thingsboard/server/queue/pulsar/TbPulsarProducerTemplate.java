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

import com.google.gson.Gson;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.*;
import org.thingsboard.server.common.msg.queue.TopicPartitionInfo;
import org.thingsboard.server.queue.TbQueueAdmin;
import org.thingsboard.server.queue.TbQueueCallback;
import org.thingsboard.server.queue.TbQueueMsg;
import org.thingsboard.server.queue.TbQueueProducer;
import org.thingsboard.server.queue.common.DefaultTbQueueMsg;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Slf4j
public class TbPulsarProducerTemplate<T extends TbQueueMsg> implements TbQueueProducer<T> {

    @Getter
    private final String defaultTopic;

    @Getter
    private final TbPulsarSettings settings;
    private final Gson gson = new Gson();
    private final PulsarClient pulsarClient;
    private final TbQueueAdmin admin;
    private final Set<TopicPartitionInfo> topics;
    private final String clientId;

    private ConcurrentMap<String, Producer<byte[]>> producerMap = new ConcurrentHashMap<>();

    private final String topicPrefix = "sangam/thingsboard/";

    @Builder
    private TbPulsarProducerTemplate(TbPulsarSettings settings, PulsarClient pulsarClient,
                                     String defaultTopic, String clientId, TbQueueAdmin admin) {
        this.pulsarClient = pulsarClient;
        this.settings = settings;
        this.clientId = clientId;
        this.defaultTopic = defaultTopic;
        this.admin = admin;
        topics = ConcurrentHashMap.newKeySet();
    }

    @Override
    public void init() {
    }

    @Override
    public void send(TopicPartitionInfo tpi, T msg, TbQueueCallback callback) {

        createTopicIfNotExist(tpi);

        String topic = tpi.getFullTopicName();
        Producer<byte[]> producer = producerMap.get(topic);
        if (producer == null) {
            producer = createProducer(tpi);
            producerMap.put(topic, producer);
        }


//        Map<String, String> header = msg.getHeaders().getData().entrySet().stream()
//                .collect(Collectors.toMap(
//                        e -> e.getKey(),
//                        e -> StringUtils.newStringUtf8(e.getValue())
//                ));

        try {
            MessageId messageId = producer.newMessage()
                    .key(msg.getKey().toString())
                    .value(gson.toJson(new DefaultTbQueueMsg(msg)).getBytes())
//                    .properties(header)
                    .send();//TODO maybe use async if there are any perf issue
            if (callback != null) {
                callback.onSuccess(null);
            }

//            CompletableFuture<Void> future = getClient(tpi.getFullTopicName()).sendAsync(message);
//            future.whenCompleteAsync((success, err) -> {
//                if (err != null) {
//                    callback.onFailure(err);
//                } else {
//                    callback.onSuccess(null);
//                }

            log.debug("Published messageId [{}] on topic [{}]", messageId, topicPrefix + tpi.getFullTopicName());
        } catch (PulsarClientException e) {
            log.warn("Producer template failure (send method wrapper): {}", e.getMessage(), e);
            if (callback != null) {
                callback.onFailure(e);
            }
            throw new RuntimeException(e);
        }
    }

    private Producer<byte[]> createProducer(TopicPartitionInfo topicInfo) {
        try {
            log.debug("Creating producer for topic [{}]", topicPrefix + topicInfo.getFullTopicName());
            return pulsarClient.newProducer()
                    .topic(topicPrefix + topicInfo.getFullTopicName())  //TODO FIXME make the tenant and namespace dynamic
                    .compressionType(CompressionType.LZ4)
                    .create();
        } catch (PulsarClientException e) {
            log.error("Error creating Pulsar Producer with PulsarAdmin client", e);
            throw new RuntimeException(e);
        }
    }

    private void createTopicIfNotExist(TopicPartitionInfo tpi) {
        if (topics.contains(tpi)) {
            return;
        }
        admin.createTopicIfNotExists(tpi.getFullTopicName());
        topics.add(tpi);
    }

    @Override
    public void stop() {
        producerMap.values().stream().filter(Objects::nonNull).map(Producer::closeAsync);
        producerMap.clear();
    }
}
