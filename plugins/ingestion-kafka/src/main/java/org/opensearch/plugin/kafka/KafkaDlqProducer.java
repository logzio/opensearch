/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Produces failed messages to a Dead Letter Queue (DLQ) Kafka topic.
 *
 * <p>When a message fails processing with error_strategy=drop, the original message
 * is written to the configured DLQ topic with error context in headers before
 * being discarded. This enables post-mortem analysis and reprocessing of failed messages.
 */
@SuppressWarnings("removal")
public class KafkaDlqProducer implements Closeable {
    private static final Logger logger = LogManager.getLogger(KafkaDlqProducer.class);

    private final Producer<byte[], byte[]> producer;
    private final String dlqTopic;

    /**
     * Constructor.
     * @param bootstrapServers Kafka bootstrap servers for the DLQ topic
     * @param dlqTopic the DLQ topic name
     */
    public KafkaDlqProducer(String bootstrapServers, String dlqTopic) {
        this.dlqTopic = dlqTopic;
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "opensearch-dlq-producer");

        final ClassLoader restore = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(KafkaPlugin.class.getClassLoader());
            this.producer = AccessController.doPrivileged(
                (PrivilegedAction<Producer<byte[], byte[]>>) () -> new KafkaProducer<>(
                    props,
                    new ByteArraySerializer(),
                    new ByteArraySerializer()
                )
            );
        } finally {
            Thread.currentThread().setContextClassLoader(restore);
        }
        logger.info("DLQ producer created for topic {}", dlqTopic);
    }

    /**
     * Constructor for testing with a pre-created producer.
     */
    protected KafkaDlqProducer(String dlqTopic, Producer<byte[], byte[]> producer) {
        this.dlqTopic = dlqTopic;
        this.producer = producer;
    }

    /**
     * Send a failed message to the DLQ topic with error context in headers.
     *
     * @param key original message key (may be null)
     * @param value original message payload
     * @param sourceTopic the source Kafka topic
     * @param partition the source partition
     * @param offset the source offset
     * @param index the target OpenSearch index
     * @param shard the target shard
     * @param errorType the error class name
     * @param errorMsg the error message
     */
    public void send(
        byte[] key,
        byte[] value,
        String sourceTopic,
        int partition,
        long offset,
        String index,
        int shard,
        String errorType,
        String errorMsg
    ) throws IOException {
        Headers headers = new RecordHeaders();
        headers.add("__dlq.error.type", errorType.getBytes(StandardCharsets.UTF_8));
        headers.add("__dlq.error.message", truncate(errorMsg, 1024).getBytes(StandardCharsets.UTF_8));
        headers.add("__dlq.source.topic", sourceTopic.getBytes(StandardCharsets.UTF_8));
        headers.add("__dlq.source.partition", String.valueOf(partition).getBytes(StandardCharsets.UTF_8));
        headers.add("__dlq.source.offset", String.valueOf(offset).getBytes(StandardCharsets.UTF_8));
        headers.add("__dlq.source.index", index.getBytes(StandardCharsets.UTF_8));
        headers.add("__dlq.source.shard", String.valueOf(shard).getBytes(StandardCharsets.UTF_8));
        headers.add("__dlq.timestamp", String.valueOf(System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8));

        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(dlqTopic, null, null, key, value, headers);

        try {
            producer.send(record).get();
            logger.debug("Message sent to DLQ topic {} (source: {}:{}/{})", dlqTopic, sourceTopic, partition, offset);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while sending to DLQ", e);
        } catch (ExecutionException e) {
            logger.error("Failed to send message to DLQ topic {}: {}", dlqTopic, e.getMessage());
            throw new IOException("Failed to send to DLQ", e.getCause());
        }
    }

    /**
     * Get the DLQ topic name.
     */
    public String getDlqTopic() {
        return dlqTopic;
    }

    @Override
    public void close() throws IOException {
        producer.close();
    }

    private static String truncate(String s, int maxLen) {
        if (s == null) return "";
        return s.length() <= maxLen ? s : s.substring(0, maxLen);
    }
}
