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
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.*;
import org.springframework.util.StopWatch;
import org.thingsboard.server.queue.TbQueueAdmin;
import org.thingsboard.server.queue.TbQueueMsg;
import org.thingsboard.server.queue.common.AbstractTbQueueConsumerTemplate;
import org.thingsboard.server.queue.common.DefaultTbQueueMsg;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

@Slf4j
public class TbPulsarConsumerTemplate<T extends TbQueueMsg> extends AbstractTbQueueConsumerTemplate<Message<byte[]>, T> {

    private final TbQueueAdmin admin;
    private final TbPulsarDecoder<T> decoder;

    private final Gson gson = new Gson();

    private final PulsarClient pulsarClient;
    private final String clientId;
    private final String groupId;
    private final String topic;
    private final List<MessageId> acknowledgeRequests = new CopyOnWriteArrayList<>();
    private final String topicPrefix = "sangam/thingsboard/"; //TODO FIXME make the tenant and namespace dynamic
    private Consumer<byte[]> consumer;

    @Builder
    private TbPulsarConsumerTemplate(TbPulsarSettings settings, TbPulsarDecoder<T> decoder,
                                     String clientId, String groupId, String topic,
                                     TbQueueAdmin admin,
                                     PulsarClient pulsarClient) {
        super(topic);
        this.pulsarClient = pulsarClient;

        this.clientId = clientId;
        this.groupId = groupId;
        this.topic = topicPrefix + topic;

        this.admin = admin;
        this.decoder = decoder;
    }

    @Override
    protected void doSubscribe(List<String> topicNames) {
        try {
            if (!topicNames.isEmpty()) {
                Set<String> topicsWithPrefix = new HashSet<>();
                topicNames.forEach(topic -> {
                    admin.createTopicIfNotExists(topicPrefix + topic);
                    topicsWithPrefix.add(topicPrefix + topic);
                });
//                topicsWithPrefix.add(this.topic);  //TODO fixme: Topics are supplied in many ways (constructor & as topicNames) to subscribe. This has to be streamlined.
                log.info("subscribe topics {} and topic {}", topicsWithPrefix, topic);
                this.consumer = pulsarClient.newConsumer()
                        .subscriptionName(groupId)
                        .subscriptionType(SubscriptionType.Shared)
                        .receiverQueueSize(1)
                        .consumerName(clientId)
//                        .topic(topic)
                        .topics(List.copyOf(topicsWithPrefix))
                        .subscribe();
            } else {
                log.info("unsubscribe due to empty topic list");
            }
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected List<Message<byte[]>> doPoll(long durationInMillis) {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        log.trace("poll topic {} maxDuration {}", getTopic(), durationInMillis);

        Messages<byte[]> messages = null;
        Message<byte[]> record = null;

        try {
            //TODO receive one message at a time or batch receive
            record = consumer.receive((int) durationInMillis, TimeUnit.MILLISECONDS);
            consumer.batchReceive();
//            messages = consumer.batchReceive();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }

        stopWatch.stop();
//        log.debug("receive topic {} and messageid {} took {} ms", getTopic(), record.getMessageId(), stopWatch.getTotalTimeMillis());

//        if (messages == null) {
        if (record == null) {
            return Collections.emptyList();
        } else {
            log.debug("receive topic {} and messageId {} took {} ms", getTopic(), record.getMessageId(), stopWatch.getTotalTimeMillis());
            List<Message<byte[]>> recordList = new ArrayList<>();
            recordList.add(record);
            acknowledgeRequests.add(record.getMessageId());

//            messages.forEach(message -> {
//                recordList.add(message);
//                acknowledgeRequests.add(message.getMessageId());
//            });
//            log.debug("Received messages with ids {}", acknowledgeRequests);
            return recordList;
        }
    }

    @Override
    public T decode(Message<byte[]> record) throws IOException {
        DefaultTbQueueMsg msg = gson.fromJson(new String(record.getValue()), DefaultTbQueueMsg.class);
        return decoder.decode(msg);
    }

    @Override
    protected void doCommit() {
        try {
            consumer.acknowledge(acknowledgeRequests);
            acknowledgeRequests.clear();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void doUnsubscribe() {
        log.info("unsubscribe topic and close consumer for topic {}", getTopic());
        if (consumer != null) {
            try {
                consumer.unsubscribe();
                consumer.close();
            } catch (PulsarClientException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
