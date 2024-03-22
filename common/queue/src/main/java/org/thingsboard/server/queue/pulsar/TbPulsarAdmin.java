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
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.thingsboard.server.queue.TbQueueAdmin;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class TbPulsarAdmin implements TbQueueAdmin {

    private final PulsarAdmin pulsarAdmin;
    private final Map<String, String> topicConfigs;
    private final Set<String> topics = ConcurrentHashMap.newKeySet();
    private final int numPartitions;
    private final TbPulsarSettings tbPulsarSettings;

    String clusterName;
    String tenant = "sangam";
    String namespace = "thingsboard";
    String fullNamespace = tenant + "/" + namespace; //persistent://sangam/thingsboard

    public TbPulsarAdmin(TbPulsarSettings tbPulsarSettings, Map<String, String> topicConfigs, PulsarAdmin pulsarAdmin) {

        this.tbPulsarSettings = tbPulsarSettings;
        this.topicConfigs = topicConfigs;
        this.pulsarAdmin = pulsarAdmin;
        this.clusterName = tbPulsarSettings.getCluster();
        try {
            initTenantAndNamespace();
            topics.addAll(pulsarAdmin.topics().getList(fullNamespace, TopicDomain.persistent));
        } catch (PulsarAdminException e) {
            log.error("Failed to get all topics.", e);
        }

        String numPartitionsStr = topicConfigs.get(TbPulsarTopicConfigs.NUM_PARTITIONS_SETTING);
        if (numPartitionsStr != null) {
            numPartitions = Integer.parseInt(numPartitionsStr);
            topicConfigs.remove("partitions");
        } else {
            numPartitions = 1;
        }
    }

    @Override
    public void createTopicIfNotExists(String topic) {
        if (topics.contains(topic)) {
            return;
        }
        try {
            createTopic(topic, numPartitions, topicConfigs);
            topics.add(topic);
        } catch (PulsarAdminException ee) {
            log.warn("[{}] Failed to create topic", topic, ee);
            throw new RuntimeException(ee);
        }
    }

    @Override
    public void createTopicIfNotExists(String topic, String properties) {
        createTopicIfNotExists(topic);
    }

    @Override
    public void deleteTopic(String topic) {
        try {
            if (topics.contains(topic)) {
                pulsarAdmin.topics().delete(topic, true); //closes all the subscription and consumers
            } else {
                if (pulsarAdmin.topics().getList(namespace, TopicDomain.persistent).contains(topic)) {
                    pulsarAdmin.topics().delete(topic, true); //closes all the subscription and consumers
                } else {
                    log.warn("Pulsar topic [{}] does not exist.", topic);
                }
            }
        } catch (PulsarAdminException e) {
            log.error("Failed to delete pulsar topic [{}].", topic, e);
        }
    }

    @Override
    public void destroy() {
        if (pulsarAdmin != null) {
            pulsarAdmin.close();
        }
    }

    private void initTenantAndNamespace() {

        //        pulsarAdmin.tenants().deleteTenant(tenant);
        try {
            if (!pulsarAdmin.tenants().getTenants().contains(tenant)) {
                log.info("Creating new tenant [{}] with PulsarAdmin", tenant);
                TenantInfo tenantInfo = TenantInfo.builder().allowedClusters(Set.of(clusterName)).build();
                pulsarAdmin.tenants().createTenant(tenant, tenantInfo);
            }
            if (!pulsarAdmin.namespaces().getNamespaces(tenant).contains(fullNamespace)) {
                RetentionPolicies retentionPolicy = new RetentionPolicies(-1, -1); //infinite
                Policies policies = new Policies();
                policies.retention_policies = retentionPolicy;

                log.info("Creating new namespace: {} for tenant: {}", namespace, tenant);
                pulsarAdmin.namespaces().createNamespace(tenant + "/" + namespace, policies);
            }
            //Initialise topic for JS Evaluator microservice which is written in TypeScript
            //and there is no pulsar admin support in the nodeJs client. So this topic creation is
            //done here. The topic name is "js_eval.requests" mentioned in the default.yml and the
            //number of partitions is derived from the property pulsar.topic_properties in the same file.
            createTopic("js_eval.requests", 50, Collections.emptyMap());
        } catch (PulsarAdminException e) {
            log.error("Failed to get all topics.", e);
        }
    }

    private void createTopic(String topic, int numPartitions, Map<String, String> topicConfigs) throws PulsarAdminException {

        if (!pulsarAdmin.clusters().getClusters().contains(clusterName)) {
            log.info("Creating new cluster: {} with PulsarAdmin.", clusterName);

            ClusterData clusterData = ClusterData.builder()
                    .serviceUrl(tbPulsarSettings.getAdmin())
                    .brokerServiceUrl(tbPulsarSettings.getBroker())
                    .build();
            pulsarAdmin.clusters().createCluster(clusterName, clusterData);
        }

        String fullTopicName = "persistent://" + fullNamespace + "/" + topic;
        //persistent://sangam/thingsboard/tb_transport.api.responses.MacBook-Pro.local-partition-0
//        pulsarAdmin.topics().delete(fullTopicName + "-partition-0", true);
//        pulsarAdmin.topics().delete(fullTopicName, true);
//        if (!pulsarAdmin.topics().getList(fullNamespace, TopicDomain.persistent).contains(fullTopicName)) {
        if (!pulsarAdmin.topics().getPartitionedTopicList(fullNamespace).contains(fullTopicName) &&
                !pulsarAdmin.topics().getList(fullNamespace, TopicDomain.persistent).contains(fullTopicName)) {
            log.info("Creating new topic: {} for namespace: {}", topic, fullNamespace);
//            pulsarAdmin.topics().createPartitionedTopic(fullTopicName, numPartitions, topicConfigs);
            pulsarAdmin.topics().createPartitionedTopic(fullTopicName, numPartitions);
//            pulsarAdmin.topics().createNonPartitionedTopic(fullTopicName, topicConfigs);
        }
    }
}
