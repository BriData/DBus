package com.creditease.dbus.heartbeat.distributed.stream;

import org.apache.kafka.streams.processor.ProcessorContext;

public interface GenericKeyValueMapper<K, V, R> {

    R apply(K key, V value, ProcessorContext context);

}
