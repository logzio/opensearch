/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.opensearch.index.IngestionShardConsumer;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class KafkaGroupConsumerTests extends OpenSearchTestCase {

    private KafkaSourceConfig config;
    private Consumer<byte[], byte[]> mockConsumer;
    private KafkaGroupConsumer consumer;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        Map<String, Object> params = new HashMap<>();
        params.put("topic", "test-topic");
        params.put("bootstrap_servers", "localhost:9092");
        params.put("consumer_mode", "subscribe");
        params.put("group.id", "test-group");

        config = new KafkaSourceConfig(1000, params);
        mockConsumer = mock(Consumer.class);
        consumer = new KafkaGroupConsumer(config, 0, mockConsumer);
    }

    public void testReadNextWithNoPartitionAssigned() throws Exception {
        // Before any partition is assigned, poll should still work (triggers rebalance)
        ConsumerRecords<byte[], byte[]> emptyRecords = new ConsumerRecords<>(Collections.emptyMap());
        when(mockConsumer.poll(any(Duration.class))).thenReturn(emptyRecords);

        List<IngestionShardConsumer.ReadResult<KafkaOffset, KafkaMessage>> result = consumer.readNext(10, 1000);
        assertEquals(0, result.size());
        assertEquals(-1, consumer.getAssignedPartitionId());
    }

    public void testReadNextAfterPartitionAssignment() throws Exception {
        // Simulate partition assignment via rebalance listener
        simulatePartitionAssignment(0);

        TopicPartition tp = new TopicPartition("test-topic", 0);
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("test-topic", 0, 5, null, "msg".getBytes(StandardCharsets.UTF_8));
        ConsumerRecords<byte[], byte[]> records = new ConsumerRecords<>(
            Collections.singletonMap(tp, Collections.singletonList(record))
        );
        when(mockConsumer.poll(any(Duration.class))).thenReturn(records);

        List<IngestionShardConsumer.ReadResult<KafkaOffset, KafkaMessage>> result = consumer.readNext(10, 1000);
        assertEquals(1, result.size());
        assertEquals(5L, result.get(0).getPointer().getOffset());
        assertEquals(0, result.get(0).getPointer().getPartitionId());
        assertEquals(0, consumer.getAssignedPartitionId());
    }

    public void testReadNextWithRecoveryPointerSamePartition() throws Exception {
        simulatePartitionAssignment(2);

        TopicPartition tp = new TopicPartition("test-topic", 2);
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("test-topic", 2, 100, null, "msg".getBytes(StandardCharsets.UTF_8));
        ConsumerRecords<byte[], byte[]> records = new ConsumerRecords<>(
            Collections.singletonMap(tp, Collections.singletonList(record))
        );
        when(mockConsumer.poll(any(Duration.class))).thenReturn(records);

        // Recovery pointer with same partition — should seek normally
        KafkaOffset recoveryPointer = new KafkaOffset(99, 2);
        List<IngestionShardConsumer.ReadResult<KafkaOffset, KafkaMessage>> result = consumer.readNext(recoveryPointer, false, 10, 1000);
        assertEquals(1, result.size());
    }

    public void testReadNextWithRecoveryPointerDifferentPartition() throws Exception {
        simulatePartitionAssignment(3);

        ConsumerRecords<byte[], byte[]> emptyRecords = new ConsumerRecords<>(Collections.emptyMap());
        when(mockConsumer.poll(any(Duration.class))).thenReturn(emptyRecords);

        // Recovery pointer from partition 1, but currently assigned partition 3 — partition changed
        KafkaOffset recoveryPointer = new KafkaOffset(500, 1);
        List<IngestionShardConsumer.ReadResult<KafkaOffset, KafkaMessage>> result = consumer.readNext(recoveryPointer, true, 10, 1000);
        // Should not throw, should just poll from current position
        assertEquals(0, result.size());
    }

    public void testGetShardId() {
        assertEquals(0, consumer.getShardId());
    }

    public void testEarliestPointerNoAssignment() {
        KafkaOffset offset = (KafkaOffset) consumer.earliestPointer();
        assertEquals(0L, offset.getOffset());
    }

    public void testLatestPointerNoAssignment() {
        KafkaOffset offset = (KafkaOffset) consumer.latestPointer();
        assertEquals(0L, offset.getOffset());
    }

    public void testEarliestPointerWithAssignment() {
        simulatePartitionAssignment(0);
        TopicPartition tp = new TopicPartition("test-topic", 0);
        when(mockConsumer.beginningOffsets(Collections.singletonList(tp)))
            .thenReturn(Collections.singletonMap(tp, 10L));

        KafkaOffset offset = (KafkaOffset) consumer.earliestPointer();
        assertEquals(10L, offset.getOffset());
        assertEquals(0, offset.getPartitionId());
    }

    public void testLatestPointerWithAssignment() {
        simulatePartitionAssignment(0);
        TopicPartition tp = new TopicPartition("test-topic", 0);
        when(mockConsumer.endOffsets(Collections.singletonList(tp)))
            .thenReturn(Collections.singletonMap(tp, 100L));

        KafkaOffset offset = (KafkaOffset) consumer.latestPointer();
        assertEquals(100L, offset.getOffset());
        assertEquals(0, offset.getPartitionId());
    }

    public void testGetPointerBasedLagNoAssignment() {
        long lag = consumer.getPointerBasedLag(new KafkaOffset(0));
        assertEquals(0L, lag);
    }

    public void testGetPointerBasedLagWithAssignment() {
        simulatePartitionAssignment(0);
        TopicPartition tp = new TopicPartition("test-topic", 0);
        when(mockConsumer.endOffsets(Collections.singletonList(tp)))
            .thenReturn(Collections.singletonMap(tp, 100L));

        long lag = consumer.getPointerBasedLag(new KafkaOffset(0));
        assertEquals(100L, lag);
    }

    public void testPointerFromTimestampNoAssignment() {
        expectThrows(IllegalStateException.class, () -> consumer.pointerFromTimestampMillis(1000L));
    }

    public void testConsumerModeConfig() {
        assertEquals("subscribe", config.getConsumerMode());
    }

    /**
     * Simulate a partition assignment by triggering the rebalance listener through
     * the consumer's subscribe callback. We do this by accessing the consumer's
     * internal state via the test-visible constructor.
     */
    private void simulatePartitionAssignment(int partitionId) {
        // Use reflection-free approach: the RebalanceListener is registered via subscribe().
        // Since we use the test constructor, the listener is already registered.
        // We manually set the state that the listener would set.
        // This is a test-only approach; in production, the Kafka broker triggers this.
        try {
            var assignedPartitionField = KafkaGroupConsumer.class.getDeclaredField("assignedPartition");
            assignedPartitionField.setAccessible(true);
            assignedPartitionField.set(consumer, new TopicPartition("test-topic", partitionId));

            var assignedPartitionIdField = KafkaGroupConsumer.class.getDeclaredField("assignedPartitionId");
            assignedPartitionIdField.setAccessible(true);
            assignedPartitionIdField.set(consumer, partitionId);
        } catch (Exception e) {
            throw new RuntimeException("Failed to simulate partition assignment", e);
        }
    }
}
