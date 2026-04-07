/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class KafkaDlqProducerTests extends OpenSearchTestCase {

    private Producer<byte[], byte[]> mockProducer;
    private KafkaDlqProducer dlqProducer;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        mockProducer = mock(Producer.class);
        dlqProducer = new KafkaDlqProducer("dlq-topic", mockProducer);
    }

    public void testSendWritesMessageWithHeaders() throws Exception {
        RecordMetadata metadata = new RecordMetadata(new TopicPartition("dlq-topic", 0), 0, 0, 0, 0, 0);
        when(mockProducer.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(metadata));

        byte[] key = "test-key".getBytes(StandardCharsets.UTF_8);
        byte[] value = "{\"bad\": \"data\"}".getBytes(StandardCharsets.UTF_8);

        dlqProducer.send(key, value, "source-topic", 3, 12345, "my-index", 0, "MapperParsingException", "failed to parse");

        verify(mockProducer).send(any(ProducerRecord.class));
    }

    public void testGetDlqTopic() {
        assertEquals("dlq-topic", dlqProducer.getDlqTopic());
    }

    public void testDlqConfigParsing() {
        Map<String, Object> params = new HashMap<>();
        params.put("topic", "source-topic");
        params.put("bootstrap_servers", "localhost:9092");
        params.put("dlq.topic", "my-dlq");
        params.put("dlq.bootstrap_servers", "dlq-kafka:9092");

        KafkaSourceConfig config = new KafkaSourceConfig(1000, params);
        assertEquals("my-dlq", config.getDlqTopic());
        assertEquals("dlq-kafka:9092", config.getDlqBootstrapServers());
    }

    public void testDlqConfigDefaultBootstrapServers() {
        Map<String, Object> params = new HashMap<>();
        params.put("topic", "source-topic");
        params.put("bootstrap_servers", "localhost:9092");
        params.put("dlq.topic", "my-dlq");

        KafkaSourceConfig config = new KafkaSourceConfig(1000, params);
        assertEquals("my-dlq", config.getDlqTopic());
        assertEquals("localhost:9092", config.getDlqBootstrapServers());
    }

    public void testDlqConfigNullWhenNotSet() {
        Map<String, Object> params = new HashMap<>();
        params.put("topic", "source-topic");
        params.put("bootstrap_servers", "localhost:9092");

        KafkaSourceConfig config = new KafkaSourceConfig(1000, params);
        assertNull(config.getDlqTopic());
    }

    public void testDlqTopicNotInConsumerConfigs() {
        Map<String, Object> params = new HashMap<>();
        params.put("topic", "source-topic");
        params.put("bootstrap_servers", "localhost:9092");
        params.put("dlq.topic", "my-dlq");
        params.put("dlq.bootstrap_servers", "dlq-kafka:9092");

        KafkaSourceConfig config = new KafkaSourceConfig(1000, params);
        assertFalse(config.getConsumerConfigurations().containsKey("dlq.topic"));
        assertFalse(config.getConsumerConfigurations().containsKey("dlq.bootstrap_servers"));
    }
}
