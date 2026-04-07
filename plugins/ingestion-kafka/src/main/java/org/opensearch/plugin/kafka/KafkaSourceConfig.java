/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.opensearch.core.util.ConfigurationUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Class encapsulating the configuration of a Kafka source.
 */
public class KafkaSourceConfig {
    private final String PROP_TOPIC = "topic";
    private final String PROP_BOOTSTRAP_SERVERS = "bootstrap_servers";
    private static final String PROP_CONSUMER_MODE = "consumer_mode";
    private static final String PROP_SHARE_ACK_MODE = "share.acknowledgement.mode";
    private static final String PROP_DLQ_TOPIC = "dlq.topic";
    private static final String PROP_DLQ_BOOTSTRAP_SERVERS = "dlq.bootstrap_servers";

    private final String topic;
    private final String bootstrapServers;
    private final String autoOffsetResetConfig;
    private final int maxPollRecords;
    private final String consumerMode;
    private final String shareAcknowledgementMode;
    private final String dlqTopic;
    private final String dlqBootstrapServers;

    private final Map<String, Object> consumerConfigsMap;

    /**
     * Extracts and look for required and optional kafka consumer configurations.
     * @param maxPollSize the maximum batch size to read in a single poll
     * @param params the configuration parameters
     */
    public KafkaSourceConfig(int maxPollSize, Map<String, Object> params) {
        this.consumerConfigsMap = new HashMap<>(params);
        this.topic = ConfigurationUtils.readStringProperty(params, PROP_TOPIC);
        this.bootstrapServers = ConfigurationUtils.readStringProperty(params, PROP_BOOTSTRAP_SERVERS);

        // 'auto.offset.reset' is handled differently for Kafka sources, with the default set to none.
        // This ensures out-of-bounds offsets throw an error, unless the user explicitly sets different value.
        this.autoOffsetResetConfig = ConfigurationUtils.readStringProperty(params, ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");

        // OpenSearch supports 'maxPollSize' setting for consumers. If user did not provide a 'max.poll.records' setting,
        // maxPollSize will be used instead.
        this.maxPollRecords = ConfigurationUtils.readIntProperty(params, ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollSize);

        this.consumerMode = ConfigurationUtils.readStringProperty(params, PROP_CONSUMER_MODE, "assign");
        this.shareAcknowledgementMode = ConfigurationUtils.readStringProperty(params, PROP_SHARE_ACK_MODE, "implicit");
        this.dlqTopic = ConfigurationUtils.readOptionalStringProperty(params, PROP_DLQ_TOPIC);
        this.dlqBootstrapServers = ConfigurationUtils.readStringProperty(params, PROP_DLQ_BOOTSTRAP_SERVERS, bootstrapServers);

        // remove metadata configurations
        consumerConfigsMap.remove(PROP_TOPIC);
        consumerConfigsMap.remove(PROP_BOOTSTRAP_SERVERS);
        consumerConfigsMap.remove(PROP_CONSUMER_MODE);
        consumerConfigsMap.remove(PROP_SHARE_ACK_MODE);
        consumerConfigsMap.remove(PROP_DLQ_TOPIC);
        consumerConfigsMap.remove(PROP_DLQ_BOOTSTRAP_SERVERS);

        // add or overwrite required configurations with defaults if not present
        consumerConfigsMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetResetConfig);
        consumerConfigsMap.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
    }

    /**
     * Get the topic name
     * @return the topic name
     */
    public String getTopic() {
        return topic;
    }

    /**
     * Get the bootstrap servers
     *
     * @return the bootstrap servers
     */
    public String getBootstrapServers() {
        return bootstrapServers;
    }

    /**
     * Get the auto offset reset configuration
     *
     * @return the auto offset reset configuration
     */
    public String getAutoOffsetResetConfig() {
        return autoOffsetResetConfig;
    }

    /**
     * Get the consumer mode: "assign" (default), "subscribe", or "share"
     */
    public String getConsumerMode() {
        return consumerMode;
    }

    /**
     * Get the share group acknowledgement mode: "implicit" (default) or "explicit"
     */
    public String getShareAcknowledgementMode() {
        return shareAcknowledgementMode;
    }

    /**
     * Get the DLQ topic name, or null if DLQ is not configured.
     */
    public String getDlqTopic() {
        return dlqTopic;
    }

    /**
     * Get the DLQ bootstrap servers. Defaults to the source bootstrap servers.
     */
    public String getDlqBootstrapServers() {
        return dlqBootstrapServers;
    }

    public Map<String, Object> getConsumerConfigurations() {
        return consumerConfigsMap;
    }
}
