/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.clients.consumer.ShareConsumer;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

/**
 * Kafka consumer using KIP-932 share group protocol.
 *
 * <p>Share groups enable queue-style consumption where multiple consumers read from
 * the same partition with per-record acknowledgment. This allows scaling ingestion
 * beyond the partition count — e.g. 100 OpenSearch shards from a 10-partition topic.
 *
 * <p><b>Limitations:</b>
 * <ul>
 *   <li>{@code seek()} is not supported — broker manages delivery</li>
 *   <li>Checkpoint recovery is best-effort — broker handles redelivery on crash</li>
 *   <li>Lag is not computable client-side — returns -1</li>
 *   <li>Message ordering is not guaranteed</li>
 *   <li>At-least-once delivery — duplicates possible, use deterministic {@code _id}</li>
 *   <li>{@code pointerFromTimestampMillis()} throws {@code UnsupportedOperationException}</li>
 *   <li>All-active mode is not supported</li>
 *   <li>Warmup uses timeout only (no lag-based completion)</li>
 * </ul>
 */
@SuppressWarnings("removal")
public class KafkaShareGroupConsumer implements IngestionShardConsumer<KafkaOffset, KafkaMessage> {
    private static final Logger logger = LogManager.getLogger(KafkaShareGroupConsumer.class);

    private final ShareConsumer<byte[], byte[]> consumer;
    private final int shardId;
    private final String topic;
    private long lastFetchedOffset = -1;
    private final boolean explicitAck;

    /**
     * Constructor.
     * @param clientId the client id
     * @param config the Kafka source config (must have group.id in consumer configs)
     * @param shardId the shard id
     */
    public KafkaShareGroupConsumer(String clientId, KafkaSourceConfig config, int shardId) {
        this.shardId = shardId;
        this.topic = config.getTopic();
        this.explicitAck = "explicit".equalsIgnoreCase(config.getShareAcknowledgementMode());
        this.consumer = createShareConsumer(clientId, config);

        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            consumer.subscribe(Collections.singletonList(topic));
            return null;
        });
        logger.info("Kafka share group consumer created for topic {} shard {}", topic, shardId);
    }

    /**
     * Constructor for testing with a pre-created consumer.
     */
    protected KafkaShareGroupConsumer(int shardId, String topic, boolean explicitAck, ShareConsumer<byte[], byte[]> consumer) {
        this.shardId = shardId;
        this.topic = topic;
        this.explicitAck = explicitAck;
        this.consumer = consumer;
        consumer.subscribe(Collections.singletonList(topic));
    }

    private static ShareConsumer<byte[], byte[]> createShareConsumer(String clientId, KafkaSourceConfig config) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);

        // group.id is required for share groups
        Object groupId = config.getConsumerConfigurations().get(ConsumerConfig.GROUP_ID_CONFIG);
        if (groupId == null) {
            throw new IllegalArgumentException("group.id is required when consumer_mode=share");
        }
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // Apply user overrides, filtering out keys incompatible with share consumers
        for (Map.Entry<String, Object> entry : config.getConsumerConfigurations().entrySet()) {
            String key = entry.getKey();
            if (!key.equals(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)
                && !key.equals(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)
                && !key.equals(ConsumerConfig.GROUP_ID_CONFIG)) {
                props.putIfAbsent(key, entry.getValue());
            }
        }

        final ClassLoader restore = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(KafkaPlugin.class.getClassLoader());
            return AccessController.doPrivileged(
                (PrivilegedAction<ShareConsumer<byte[], byte[]>>) () -> new KafkaShareConsumer<>(
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
        // Share consumers cannot seek — offset parameter is ignored
        return fetchAndAcknowledge(timeoutMillis);
    }

    @Override
    public synchronized List<ReadResult<KafkaOffset, KafkaMessage>> readNext(
        long maxMessages,
        int timeoutMillis
    ) throws TimeoutException {
        return fetchAndAcknowledge(timeoutMillis);
    }

    private List<ReadResult<KafkaOffset, KafkaMessage>> fetchAndAcknowledge(int timeoutMillis) {
        ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(timeoutMillis));
        List<ReadResult<KafkaOffset, KafkaMessage>> results = new ArrayList<>();

        for (ConsumerRecord<byte[], byte[]> record : records) {
            lastFetchedOffset = Math.max(lastFetchedOffset, record.offset());

            results.add(new ReadResult<>(
                new KafkaOffset(record.offset()),
                new KafkaMessage(record.key(), record.value(), record.timestamp())
            ));

            if (explicitAck) {
                consumer.acknowledge(record, AcknowledgeType.ACCEPT);
            }
        }

        if (explicitAck && !results.isEmpty()) {
            consumer.commitSync();
        }

        return results;
    }

    @Override
    public IngestionShardPointer earliestPointer() {
        return new KafkaOffset(0);
    }

    @Override
    public IngestionShardPointer latestPointer() {
        return new KafkaOffset(Math.max(0, lastFetchedOffset));
    }

    @Override
    public IngestionShardPointer pointerFromTimestampMillis(long timestampMillis) {
        throw new UnsupportedOperationException("pointerFromTimestampMillis is not supported in share group mode");
    }

    @Override
    public IngestionShardPointer pointerFromOffset(String offset) {
        return new KafkaOffset(Long.parseLong(offset));
    }

    @Override
    public int getShardId() {
        return shardId;
    }

    /**
     * Returns -1 because lag is broker-managed in share groups and not computable client-side.
     * The server's warmup logic handles -1 correctly (falls back to timeout-only warmup).
     */
    @Override
    public long getPointerBasedLag(IngestionShardPointer expectedStartPointer) {
        return -1;
    }

    @Override
    public void close() throws IOException {
        consumer.close();
    }
}
