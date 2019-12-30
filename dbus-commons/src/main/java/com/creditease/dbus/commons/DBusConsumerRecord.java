/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2019 Bridata
 * ==
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * >>
 */


package com.creditease.dbus.commons;

/**
 * Created by dongwang47 on 2018/1/3.
 */

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;

import java.io.Serializable;

/**
 * A key/value pair to be received from Kafka. This consists of a topic name and a partition number, from which the
 * record is being received and an offset that points to the record in a Kafka partition.
 */
public final class DBusConsumerRecord<K, V> implements Serializable {
    public static final long NO_TIMESTAMP = -1;
    public static final int NULL_SIZE = -1;
    public static final int NULL_CHECKSUM = -1;

    private final String topic;
    private final int partition;
    private final long offset;
    private final long timestamp;
    private final TimestampType timestampType;
    private final long checksum;
    private final int serializedKeySize;
    private final int serializedValueSize;
    private final K key;
    private final V value;

    public DBusConsumerRecord() {
        this.topic = "";
        this.partition = 0;
        this.offset = 0;
        this.timestamp = 0;
        this.timestampType = TimestampType.NO_TIMESTAMP_TYPE;
        this.checksum = 0;
        this.serializedKeySize = 0;
        this.serializedValueSize = 0;
        this.key = null;
        this.value = null;
    }

    public DBusConsumerRecord(ConsumerRecord<K, V> record) {
        this(record.topic(), record.partition(), record.offset(), record.timestamp(), record.timestampType(),
                record.checksum(), record.serializedKeySize(), record.serializedValueSize(), record.key(), record.value());
    }

    /**
     * Creates a record to be received from a specified topic and partition
     *
     * @param topic               The topic this record is received from
     * @param partition           The partition of the topic this record is received from
     * @param offset              The offset of this record in the corresponding Kafka partition
     * @param timestamp           The timestamp of the record.
     * @param timestampType       The timestamp type
     * @param checksum            The checksum (CRC32) of the full record
     * @param serializedKeySize   The length of the serialized key
     * @param serializedValueSize The length of the serialized value
     * @param key                 The key of the record, if one exists (null is allowed)
     * @param value               The record contents
     */
    public DBusConsumerRecord(String topic,
                              int partition,
                              long offset,
                              long timestamp,
                              TimestampType timestampType,
                              long checksum,
                              int serializedKeySize,
                              int serializedValueSize,
                              K key,
                              V value) {
        if (topic == null)
            throw new IllegalArgumentException("Topic cannot be null");
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
        this.timestampType = timestampType;
        this.checksum = checksum;
        this.serializedKeySize = serializedKeySize;
        this.serializedValueSize = serializedValueSize;
        this.key = key;
        this.value = value;
    }

    /**
     * The topic this record is received from
     */
    public String topic() {
        return this.topic;
    }

    /**
     * The partition from which this record is received
     */
    public int partition() {
        return this.partition;
    }

    /**
     * The key (or null if no key is specified)
     */
    public K key() {
        return key;
    }

    /**
     * The value
     */
    public V value() {
        return value;
    }

    /**
     * The position of this record in the corresponding Kafka partition.
     */
    public long offset() {
        return offset;
    }

    /**
     * The timestamp of this record
     */
    public long timestamp() {
        return timestamp;
    }

    /**
     * The timestamp type of this record
     */
    public TimestampType timestampType() {
        return timestampType;
    }

    /**
     * The checksum (CRC32) of the record.
     */
    public long checksum() {
        return this.checksum;
    }

    /**
     * The size of the serialized, uncompressed key in bytes. If key is null, the returned size
     * is -1.
     */
    public int serializedKeySize() {
        return this.serializedKeySize;
    }

    /**
     * The size of the serialized, uncompressed value in bytes. If value is null, the
     * returned size is -1.
     */
    public int serializedValueSize() {
        return this.serializedValueSize;
    }


    @Override
    public String toString() {
        return "ConsumerRecord(topic = " + topic() + ", partition = " + partition() + ", offset = " + offset()
                + ", " + timestampType + " = " + timestamp + ", checksum = " + checksum
                + ", serialized key size = " + serializedKeySize
                + ", serialized value size = " + serializedValueSize
                + ", key = " + key + ", value = " + value + ")";
    }
}
