/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.common.TopicPartition;
import org.opensearch.index.IngestionShardConsumer;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class KafkaShareGroupConsumerTests extends OpenSearchTestCase {

    private ShareConsumer<byte[], byte[]> mockConsumer;
    private KafkaShareGroupConsumer consumer;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        mockConsumer = mock(ShareConsumer.class);
        consumer = new KafkaShareGroupConsumer(0, "test-topic", false, mockConsumer);
    }

    public void testReadNextIgnoresOffset() throws Exception {
        TopicPartition tp = new TopicPartition("test-topic", 0);
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("test-topic", 0, 42, null, "msg".getBytes(StandardCharsets.UTF_8));
        ConsumerRecords<byte[], byte[]> records = new ConsumerRecords<>(
            Collections.singletonMap(tp, Collections.singletonList(record))
        );
        when(mockConsumer.poll(any(Duration.class))).thenReturn(records);

        // Even with an offset pointer, share consumer ignores it and just polls
        KafkaOffset pointer = new KafkaOffset(999);
        List<IngestionShardConsumer.ReadResult<KafkaOffset, KafkaMessage>> result = consumer.readNext(pointer, true, 10, 1000);
        assertEquals(1, result.size());
        assertEquals(42L, result.get(0).getPointer().getOffset());
    }

    public void testReadNextWithoutOffset() throws Exception {
        TopicPartition tp = new TopicPartition("test-topic", 0);
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("test-topic", 0, 10, null, "msg".getBytes(StandardCharsets.UTF_8));
        ConsumerRecords<byte[], byte[]> records = new ConsumerRecords<>(
            Collections.singletonMap(tp, Collections.singletonList(record))
        );
        when(mockConsumer.poll(any(Duration.class))).thenReturn(records);

        List<IngestionShardConsumer.ReadResult<KafkaOffset, KafkaMessage>> result = consumer.readNext(10, 1000);
        assertEquals(1, result.size());
        assertEquals("msg", new String(result.get(0).getMessage().getPayload(), StandardCharsets.UTF_8));
    }

    public void testEarliestPointerReturnsZero() {
        KafkaOffset offset = (KafkaOffset) consumer.earliestPointer();
        assertEquals(0L, offset.getOffset());
    }

    public void testLatestPointerReturnsLastFetched() throws Exception {
        // Before any fetch, latest should be 0
        KafkaOffset before = (KafkaOffset) consumer.latestPointer();
        assertEquals(0L, before.getOffset());

        // After fetch, latest should be max offset seen
        TopicPartition tp = new TopicPartition("test-topic", 0);
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("test-topic", 0, 50, null, "msg".getBytes(StandardCharsets.UTF_8));
        ConsumerRecords<byte[], byte[]> records = new ConsumerRecords<>(
            Collections.singletonMap(tp, Collections.singletonList(record))
        );
        when(mockConsumer.poll(any(Duration.class))).thenReturn(records);
        consumer.readNext(10, 1000);

        KafkaOffset after = (KafkaOffset) consumer.latestPointer();
        assertEquals(50L, after.getOffset());
    }

    public void testPointerFromTimestampThrows() {
        expectThrows(UnsupportedOperationException.class, () -> consumer.pointerFromTimestampMillis(1000L));
    }

    public void testPointerFromOffset() {
        KafkaOffset offset = (KafkaOffset) consumer.pointerFromOffset("123");
        assertEquals(123L, offset.getOffset());
    }

    public void testGetShardId() {
        assertEquals(0, consumer.getShardId());
    }

    public void testGetPointerBasedLagReturnsNegativeOne() {
        long lag = consumer.getPointerBasedLag(new KafkaOffset(0));
        assertEquals(-1L, lag);
    }

    public void testLastFetchedOffsetTracksMax() throws Exception {
        TopicPartition tp = new TopicPartition("test-topic", 0);

        // First batch with offset 100
        ConsumerRecord<byte[], byte[]> r1 = new ConsumerRecord<>("test-topic", 0, 100, null, "a".getBytes(StandardCharsets.UTF_8));
        when(mockConsumer.poll(any(Duration.class))).thenReturn(
            new ConsumerRecords<>(Collections.singletonMap(tp, Collections.singletonList(r1)))
        );
        consumer.readNext(10, 1000);
        assertEquals(100L, ((KafkaOffset) consumer.latestPointer()).getOffset());

        // Second batch with offset 50 (out of order from different partition)
        ConsumerRecord<byte[], byte[]> r2 = new ConsumerRecord<>("test-topic", 1, 50, null, "b".getBytes(StandardCharsets.UTF_8));
        when(mockConsumer.poll(any(Duration.class))).thenReturn(
            new ConsumerRecords<>(Collections.singletonMap(new TopicPartition("test-topic", 1), Collections.singletonList(r2)))
        );
        consumer.readNext(10, 1000);

        // Max should still be 100
        assertEquals(100L, ((KafkaOffset) consumer.latestPointer()).getOffset());
    }

    public void testShareAcknowledgementModeConfig() {
        // Default config
        java.util.Map<String, Object> params = new java.util.HashMap<>();
        params.put("topic", "t");
        params.put("bootstrap_servers", "localhost:9092");
        KafkaSourceConfig defaultConfig = new KafkaSourceConfig(1000, params);
        assertEquals("implicit", defaultConfig.getShareAcknowledgementMode());

        // Explicit config
        params.put("share.acknowledgement.mode", "explicit");
        KafkaSourceConfig explicitConfig = new KafkaSourceConfig(1000, params);
        assertEquals("explicit", explicitConfig.getShareAcknowledgementMode());
    }
}
