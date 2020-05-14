package com.creditease.dbus.heartbeat.distributed.stream.transformer;

import com.creditease.dbus.commons.StatMessage;
import com.creditease.dbus.heartbeat.distributed.stream.HBKeySupplier;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;

public class StatTransformer implements Transformer<HBKeySupplier.HBKey, String, KeyValue<String, String>> {

    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public KeyValue<String, String> transform(HBKeySupplier.HBKey key, String value) {
        long curTime =  System.currentTimeMillis();
        StatMessage sm = new StatMessage(key.getDs(), key.getSchema(), key.getTable(), "HEART_BEAT");
        sm.setCheckpointMS(key.getCheckpointMs());
        sm.setTxTimeMS(key.getTxTimeMs());
        sm.setLocalMS(curTime);
        sm.setLatencyMS(curTime - key.getCheckpointMs());
        sm.setOffset(context.offset());
        return new KeyValue(key.getOriginKey(), sm.toJSONString());
    }

    @Override
    public KeyValue<String, String> punctuate(long timestamp) {
        return null;
    }

    @Override
    public void close() {

    }
}
