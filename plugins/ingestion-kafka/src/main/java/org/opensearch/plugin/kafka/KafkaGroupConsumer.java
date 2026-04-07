/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.IngestionShardConsumer;
import org.opensearch.index.IngestionShardPointer;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

/**
 * Kafka consumer using KIP-848 consumer group protocol (subscribe mode).
 *
 * <p>Instead of manual partition assignment, this consumer subscribes to a topic
 * and lets the broker assign partitions via the new consumer group protocol.
 * This provides resilience to Kafka topic repartitioning and incremental rebalance.
 *
 * <p>Design constraint: each consumer gets at most 1 partition (shards >= partitions).
 * The checkpoint encodes the partition ID alongside the offset ("partitionId:offset")
 * to detect partition reassignment on recovery.
 */
@SuppressWarnings("removal")
public class KafkaGroupConsumer implements IngestionShardConsumer<KafkaOffset, KafkaMessage> {
    private static final Logger logger = LogManager.getLogger(KafkaGroupConsumer.class);

    private final Consumer<byte[], byte[]> consumer;
    private final int shardId;
    private final KafkaSourceConfig config;
    private long lastFetchedOffset = -1;

    private volatile TopicPartition assignedPartition = null;
    private volatile int assignedPartitionId = -1;

    /**
     * Constructor.
     * @param clientId the client id
     * @param config the Kafka source config (must have group.id in consumer configs)
     * @param shardId the shard id
     */
    public KafkaGroupConsumer(String clientId, KafkaSourceConfig config, int shardId) {
        this.shardId = shardId;
        this.config = config;
        this.consumer = createGroupConsumer(clientId, config);

        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            consumer.subscribe(Collections.singletonList(config.getTopic()), new RebalanceListener());
            return null;
        });
        logger.info("Kafka group consumer created for topic {} shard {}", config.getTopic(), shardId);
    }

    /**
     * Constructor for testing with a pre-created consumer.
     */
    protected KafkaGroupConsumer(KafkaSourceConfig config, int shardId, Consumer<byte[], byte[]> consumer) {
        this.shardId = shardId;
        this.config = config;
        this.consumer = consumer;
        consumer.subscribe(Collections.singletonList(config.getTopic()), new RebalanceListener());
    }

    private static Consumer<byte[], byte[]> createGroupConsumer(String clientId, KafkaSourceConfig config) {
        Properties props = KafkaPartitionConsumer.createConsumerProperties(clientId, config);
        props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "consumer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        if (!props.containsKey(ConsumerConfig.GROUP_ID_CONFIG)) {
            throw new IllegalArgumentException("group.id is required when consumer_mode=subscribe");
        }

        final ClassLoader restore = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(KafkaPlugin.class.getClassLoader());
            return AccessController.doPrivileged(
                (PrivilegedAction<Consumer<byte[], byte[]>>) () -> new KafkaConsumer<>(
                    props,
                    new ByteArrayDeserializer(),
                    new ByteArrayDeserializer()
                )
            );
        } finally {
            Thread.currentThread().setContextClassLoader(restore);
        }
    }

    @Override
    public synchronized List<ReadResult<KafkaOffset, KafkaMessage>> readNext(
        KafkaOffset offset,
        boolean includeStart,
        long maxMessages,
        int timeoutMillis
    ) throws TimeoutException {
        if (assignedPartition == null) {
            return fetchRecords(timeoutMillis);
        }

        // Check for partition reassignment: if the recovered pointer has a different
        // partition than what we're currently assigned, we can't seek to that offset.
        if (offset.getPartitionId() >= 0 && offset.getPartitionId() != assignedPartitionId) {
            logger.warn(
                "Partition assignment changed from {} to {} for shard {}. "
                    + "Stored offset {} is invalid for new partition. Consuming from current position.",
                offset.getPartitionId(),
                assignedPartitionId,
                shardId,
                offset.getOffset()
            );
            return fetchRecords(timeoutMillis);
        }

        long targetOffset = includeStart ? offset.getOffset() : offset.getOffset() + 1;
        if (lastFetchedOffset < 0 || lastFetchedOffset != targetOffset - 1) {
            consumer.seek(assignedPartition, targetOffset);
            lastFetchedOffset = targetOffset - 1;
        }
        return fetchRecords(timeoutMillis);
    }

    @Override
    public synchronized List<ReadResult<KafkaOffset, KafkaMessage>> readNext(
        long maxMessages,
        int timeoutMillis
    ) throws TimeoutException {
        return fetchRecords(timeoutMillis);
    }

    private List<ReadResult<KafkaOffset, KafkaMessage>> fetchRecords(int timeoutMillis) {
        ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(timeoutMillis));
        List<ReadResult<KafkaOffset, KafkaMessage>> results = new ArrayList<>();
        for (ConsumerRecord<byte[], byte[]> record : records) {
            lastFetchedOffset = record.offset();
            results.add(new ReadResult<>(
                new KafkaOffset(record.offset(), record.partition()),
                new KafkaMessage(record.key(), record.value(), record.timestamp())
            ));
        }
        return results;
    }

    @Override
    public IngestionShardPointer earliestPointer() {
        if (assignedPartition == null) return new KafkaOffset(0);
        long start = consumer.beginningOffsets(Collections.singletonList(assignedPartition))
            .getOrDefault(assignedPartition, 0L);
        return new KafkaOffset(start, assignedPartitionId);
    }

    @Override
    public IngestionShardPointer latestPointer() {
        if (assignedPartition == null) return new KafkaOffset(0);
        long end = consumer.endOffsets(Collections.singletonList(assignedPartition))
            .getOrDefault(assignedPartition, 0L);
        return new KafkaOffset(end, assignedPartitionId);
    }

    @Override
    public IngestionShardPointer pointerFromTimestampMillis(long timestampMillis) {
        if (assignedPartition == null) {
            throw new IllegalStateException("No partition assigned yet");
        }
        Map<TopicPartition, OffsetAndTimestamp> offsets = consumer.offsetsForTimes(
            Map.of(assignedPartition, timestampMillis)
        );
        OffsetAndTimestamp ot = offsets.get(assignedPartition);
        if (ot != null) {
            return new KafkaOffset(ot.offset(), assignedPartitionId);
        }
        String autoOffsetResetConfig = config.getAutoOffsetResetConfig();
        if ("earliest".equals(autoOffsetResetConfig)) {
            return earliestPointer();
        } else if ("latest".equals(autoOffsetResetConfig)) {
            return latestPointer();
        }
        throw new IllegalArgumentException("No message found for timestamp " + timestampMillis);
    }

    @Override
    public IngestionShardPointer pointerFromOffset(String offset) {
        return new KafkaOffset(Long.parseLong(offset), assignedPartitionId);
    }

    @Override
    public int getShardId() {
        return shardId;
    }

    /**
     * Returns the currently assigned partition ID, or -1 if none.
     */
    public int getAssignedPartitionId() {
        return assignedPartitionId;
    }

    @Override
    public long getPointerBasedLag(IngestionShardPointer expectedStartPointer) {
        if (assignedPartition == null) return 0;
        try {
            long endOffset = consumer.endOffsets(Collections.singletonList(assignedPartition))
                .getOrDefault(assignedPartition, 0L);
            if (lastFetchedOffset < 0) {
                long start = ((KafkaOffset) expectedStartPointer).getOffset();
                return Math.max(0, endOffset - start);
            }
            return Math.max(0, endOffset - lastFetchedOffset - 1);
        } catch (Exception e) {
            logger.warn("Failed to calculate lag for shard {}: {}", shardId, e.getMessage());
            return -1;
        }
    }

    @Override
    public void close() throws IOException {
        consumer.close();
    }

    /**
     * Rebalance listener for KIP-848 incremental rebalance protocol.
     *
     * <p>Design constraint: with shards >= partitions, each consumer gets at most 1 partition.
     * If the broker assigns more than 1, we log a warning and only consume from the first.
     */
    private class RebalanceListener implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            if (assignedPartition != null && partitions.contains(assignedPartition)) {
                logger.info("Partition {} revoked from shard {}", assignedPartition, shardId);
                assignedPartition = null;
                assignedPartitionId = -1;
                lastFetchedOffset = -1;
            }
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            if (partitions.isEmpty()) {
                logger.info("Shard {} has no partition assigned (idle)", shardId);
                return;
            }
            if (partitions.size() > 1) {
                logger.warn(
                    "Shard {} assigned {} partitions, expected at most 1. Consider adding more shards.",
                    shardId,
                    partitions.size()
                );
            }
            TopicPartition tp = partitions.iterator().next();
            assignedPartition = tp;
            assignedPartitionId = tp.partition();
            lastFetchedOffset = -1;
            logger.info("Shard {} assigned partition {}", shardId, tp.partition());
        }
    }
}
