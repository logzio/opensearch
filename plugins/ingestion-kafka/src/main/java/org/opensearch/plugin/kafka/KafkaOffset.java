/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.search.Query;
import org.opensearch.index.IngestionShardPointer;

import java.nio.ByteBuffer;

/**
 * Kafka offset, optionally associated with a partition ID.
 *
 * <p>In assign mode, partitionId is -1 and asString() returns the offset only (e.g. "12345").
 * In subscribe mode, partitionId is set and asString() returns "partitionId:offset" (e.g. "2:12345").
 * This encoding is stored in Lucene commit data and parsed back by
 * {@link KafkaConsumerFactory#parsePointerFromString(String)}.
 */
public class KafkaOffset implements IngestionShardPointer {

    private final long offset;
    private final int partitionId;

    /**
     * Constructor for assign mode (no partition tracking).
     * @param offset the offset
     */
    public KafkaOffset(long offset) {
        this(offset, -1);
    }

    /**
     * Constructor with partition ID for subscribe mode.
     * @param offset the offset
     * @param partitionId the partition ID, or -1 for assign mode
     */
    public KafkaOffset(long offset, int partitionId) {
        assert offset >= 0;
        this.offset = offset;
        this.partitionId = partitionId;
    }

    @Override
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(offset);
        return buffer.array();
    }

    @Override
    public String asString() {
        if (partitionId >= 0) {
            return partitionId + ":" + offset;
        }
        return String.valueOf(offset);
    }

    @Override
    public Field asPointField(String fieldName) {
        return new LongPoint(fieldName, offset);
    }

    @Override
    public Query newRangeQueryGreaterThan(String fieldName) {
        return LongPoint.newRangeQuery(fieldName, offset, Long.MAX_VALUE);
    }

    /**
     * Get the offset
     * @return the offset
     */
    public long getOffset() {
        return offset;
    }

    /**
     * Get the partition ID. Returns -1 in assign mode (partition implicit from shard ID).
     * @return the partition ID
     */
    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public String toString() {
        if (partitionId >= 0) {
            return "KafkaOffset{partition=" + partitionId + ", offset=" + offset + '}';
        }
        return "KafkaOffset{" + "offset=" + offset + '}';
    }

    @Override
    public int compareTo(IngestionShardPointer o) {
        if (o == null) {
            throw new IllegalArgumentException("the pointer is null");
        }
        if (!(o instanceof KafkaOffset other)) {
            throw new IllegalArgumentException("the pointer is of type " + o.getClass() + " and not KafkaOffset");
        }
        return Long.compare(offset, other.offset);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KafkaOffset that = (KafkaOffset) o;
        return offset == that.offset;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(offset);
    }
}
