/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

import org.opensearch.cluster.metadata.IngestionSource;
import org.opensearch.index.IngestionConsumerFactory;
import org.opensearch.index.IngestionShardConsumer;

/**
 * Factory for creating Kafka consumers. Routes to the appropriate consumer
 * implementation based on the configured consumer_mode.
 */
public class KafkaConsumerFactory
    implements IngestionConsumerFactory<IngestionShardConsumer<KafkaOffset, KafkaMessage>, KafkaOffset> {

    /**
     * Configuration for the Kafka source
     */
    protected KafkaSourceConfig config;

    /**
     * Constructor.
     */
    public KafkaConsumerFactory() {}

    @Override
    public void initialize(IngestionSource ingestionSource) {
        config = new KafkaSourceConfig((int) ingestionSource.getMaxPollSize(), ingestionSource.params());
    }

    @Override
    public IngestionShardConsumer<KafkaOffset, KafkaMessage> createShardConsumer(String clientId, int shardId) {
        assert config != null;
        String mode = config.getConsumerMode();
        return switch (mode.toLowerCase()) {
            case "subscribe" -> new KafkaGroupConsumer(clientId, config, shardId);
            case "share" -> new KafkaShareGroupConsumer(clientId, config, shardId);
            default -> new KafkaPartitionConsumer(clientId, config, shardId);
        };
    }

    /**
     * Parse a pointer from its string representation. Supports both formats:
     * <ul>
     *   <li>Legacy assign mode: "12345" (offset only, partitionId = -1)</li>
     *   <li>Subscribe mode: "2:12345" (partitionId:offset)</li>
     * </ul>
     */
    @Override
    public KafkaOffset parsePointerFromString(String pointer) {
        if (pointer.contains(":")) {
            String[] parts = pointer.split(":", 2);
            int partitionId = Integer.parseInt(parts[0]);
            long offset = Long.parseLong(parts[1]);
            return new KafkaOffset(offset, partitionId);
        }
        return new KafkaOffset(Long.parseLong(pointer));
    }
}
