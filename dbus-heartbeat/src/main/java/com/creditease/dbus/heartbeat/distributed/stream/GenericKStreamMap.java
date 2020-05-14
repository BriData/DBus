package com.creditease.dbus.heartbeat.distributed.stream;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;

public class GenericKStreamMap<K, V, K1, V1> implements ProcessorSupplier<K, V> {

    private GenericKeyValueMapper<K, V, KeyValue<K1, V1>> mapper;

    public GenericKStreamMap(GenericKeyValueMapper<K, V, KeyValue<K1, V1>> mapper) {
        this.mapper = mapper;
    }

    @Override
    public Processor get() {
        return new GenericKStreamMapProcessor();
    }

    class GenericKStreamMapProcessor extends AbstractProcessor<K, V> {
        @Override
        public void process(K key, V value) {
            KeyValue<K1, V1> newPair = mapper.apply(key, value, context());
            context().forward(newPair.key, newPair.value);
        }
    }

}
