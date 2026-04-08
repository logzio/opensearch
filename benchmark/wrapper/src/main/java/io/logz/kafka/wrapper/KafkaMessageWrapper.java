package io.logz.kafka.wrapper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Kafka Message Wrapper for ingestion-kafka benchmarking.
 *
 * Consumes raw JSON messages from a source topic (potentially on a different Kafka cluster),
 * wraps each in the ingestion-kafka envelope format, and produces to a target topic.
 *
 * The _id is generated as a composite SHA-256 hash of:
 *   logzio-signature + @timestamp + source-partition + source-offset
 * This guarantees uniqueness (partition:offset is unique per topic) and determinism
 * (reprocessing the same message produces the same _id for deduplication).
 */
public class KafkaMessageWrapper {
    private static final Logger logger = LoggerFactory.getLogger(KafkaMessageWrapper.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    private static final String SOURCE_TOPIC = getEnv("SOURCE_TOPIC");
    private static final String SOURCE_BOOTSTRAP = getEnv("SOURCE_BOOTSTRAP_SERVERS");
    private static final String TARGET_TOPIC = getEnv("TARGET_TOPIC");
    private static final String TARGET_BOOTSTRAP = getEnv("TARGET_BOOTSTRAP_SERVERS");
    private static final String GROUP_ID = getEnv("GROUP_ID", "opensearch-wrapper-benchmark");
    private static final String ID_FIELD = getEnv("ID_FIELD", "logzio-signature");
    private static final String TIMESTAMP_FIELD = getEnv("TIMESTAMP_FIELD", "@timestamp");
    private static final int POLL_TIMEOUT_MS = Integer.parseInt(getEnv("POLL_TIMEOUT_MS", "1000"));

    private static final AtomicBoolean running = new AtomicBoolean(true);
    private static final AtomicLong processedCount = new AtomicLong(0);
    private static final AtomicLong errorCount = new AtomicLong(0);

    public static void main(String[] args) {
        logger.info("Starting Kafka Message Wrapper");
        logger.info("Source: {} @ {}", SOURCE_TOPIC, SOURCE_BOOTSTRAP);
        logger.info("Target: {} @ {}", TARGET_TOPIC, TARGET_BOOTSTRAP);
        logger.info("ID fields: {} + {} + partition + offset", ID_FIELD, TIMESTAMP_FIELD);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down... processed={}, errors={}", processedCount.get(), errorCount.get());
            running.set(false);
        }));

        try (
            KafkaConsumer<byte[], byte[]> consumer = createConsumer();
            KafkaProducer<byte[], byte[]> producer = createProducer()
        ) {
            consumer.subscribe(Collections.singletonList(SOURCE_TOPIC));
            logger.info("Subscribed to {}", SOURCE_TOPIC);

            while (running.get()) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(POLL_TIMEOUT_MS));

                for (ConsumerRecord<byte[], byte[]> record : records) {
                    try {
                        byte[] envelope = wrapMessage(record);
                        String id = extractIdForPartitioning(record);

                        ProducerRecord<byte[], byte[]> output = new ProducerRecord<>(
                            TARGET_TOPIC,
                            null,                                       // partition (let Kafka decide via key hash)
                            null,                                       // timestamp (use current)
                            id.getBytes(StandardCharsets.UTF_8),        // key (_id for consistent partitioning)
                            envelope                                    // value (envelope JSON)
                        );

                        producer.send(output);
                        long count = processedCount.incrementAndGet();
                        if (count % 100_000 == 0) {
                            logger.info("Processed {} messages, errors={}", count, errorCount.get());
                        }
                    } catch (Exception e) {
                        errorCount.incrementAndGet();
                        if (errorCount.get() % 1000 == 1) {
                            logger.warn("Error wrapping message at {}:{} - {}",
                                record.partition(), record.offset(), e.getMessage());
                        }
                    }
                }
            }
        }

        logger.info("Wrapper stopped. Total processed={}, errors={}", processedCount.get(), errorCount.get());
    }

    /**
     * Wrap a raw Kafka message in the ingestion-kafka envelope format.
     *
     * Output: {"_id": "<sha256_20>", "_op_type": "index", "_source": {original JSON}}
     */
    static byte[] wrapMessage(ConsumerRecord<byte[], byte[]> record) throws Exception {
        String id = generateId(record);

        // Parse original value as JSON
        JsonNode source = mapper.readTree(record.value());

        // Build envelope
        ObjectNode envelope = mapper.createObjectNode();
        envelope.put("_id", id);
        envelope.put("_op_type", "index");
        envelope.set("_source", source);

        return mapper.writeValueAsBytes(envelope);
    }

    /**
     * Generate a deterministic, unique _id from the message.
     *
     * Formula: SHA-256( logzio-signature + @timestamp + partition + offset )
     * Truncated to 20 hex chars (80 bits — collision probability negligible at <1B docs).
     *
     * Fallback if JSON fields missing: SHA-256( partition + ":" + offset )
     * This is still globally unique per Kafka topic.
     */
    static String generateId(ConsumerRecord<byte[], byte[]> record) {
        try {
            JsonNode json = mapper.readTree(record.value());

            String signature = "";
            String timestamp = "";

            JsonNode sigNode = json.get(ID_FIELD);
            if (sigNode != null && !sigNode.isNull()) {
                signature = sigNode.asText();
            }

            JsonNode tsNode = json.get(TIMESTAMP_FIELD);
            if (tsNode != null && !tsNode.isNull()) {
                timestamp = tsNode.asText();
            }

            if (!signature.isEmpty() || !timestamp.isEmpty()) {
                String composite = signature + "|" + timestamp + "|" + record.partition() + "|" + record.offset();
                return sha256Hex(composite, 20);
            }
        } catch (Exception e) {
            // Fall through to partition:offset fallback
        }

        // Fallback: partition:offset is globally unique per topic
        String fallback = record.partition() + ":" + record.offset();
        return sha256Hex(fallback, 20);
    }

    /**
     * Extract the _id for use as the producer record key (for consistent partition hashing).
     * Same logic as generateId but avoids double-parsing.
     */
    static String extractIdForPartitioning(ConsumerRecord<byte[], byte[]> record) {
        return generateId(record);
    }

    private static String sha256Hex(String input, int hexLength) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder();
            for (byte b : hash) {
                hex.append(String.format("%02x", b));
                if (hex.length() >= hexLength) break;
            }
            return hex.substring(0, Math.min(hexLength, hex.length()));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not available", e);
        }
    }

    private static KafkaConsumer<byte[], byte[]> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SOURCE_BOOTSTRAP);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "65536");
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "500");
        return new KafkaConsumer<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    private static KafkaProducer<byte[], byte[]> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, TARGET_BOOTSTRAP);
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "65536");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, String.valueOf(64 * 1024 * 1024));
        return new KafkaProducer<>(props, new ByteArraySerializer(), new ByteArraySerializer());
    }

    private static String getEnv(String name) {
        String val = System.getenv(name);
        if (val == null || val.isEmpty()) {
            throw new IllegalArgumentException("Required env var " + name + " is not set");
        }
        return val;
    }

    private static String getEnv(String name, String defaultValue) {
        String val = System.getenv(name);
        return (val != null && !val.isEmpty()) ? val : defaultValue;
    }
}
