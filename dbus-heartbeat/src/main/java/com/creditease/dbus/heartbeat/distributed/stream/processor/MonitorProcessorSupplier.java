package com.creditease.dbus.heartbeat.distributed.stream.processor;

import com.alibaba.fastjson.JSON;
import com.creditease.dbus.heartbeat.container.CuratorContainer;
import com.creditease.dbus.heartbeat.vo.PacketVo;

import org.apache.curator.framework.CuratorFramework;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

public class MonitorProcessorSupplier implements ProcessorSupplier<String, PacketVo> {

    @Override
    public Processor get() {
        return new MonitorProcessor();
    }

    public class MonitorProcessor extends AbstractProcessor<String, PacketVo> {

        private KeyValueStore<String, String> store;

        @Override
        public void init(ProcessorContext context) {
            super.init(context);
            // 两分钟执行一次punctuate方法
            context().schedule(2 * 60 * 1000);
            this.store = (KeyValueStore<String, String>) context.getStateStore("zkInfo");
        }

        @Override
        public void process(String key, PacketVo value) {
            store.put(key, JSON.toJSONString(value));
        }

        @Override
        public void punctuate(long timestamp) {
            CuratorFramework curator = CuratorContainer.getInstance().getCurator();
            KeyValueIterator<String, String> iter = store.all();
            while (iter.hasNext()) {
                KeyValue<String, String> entry = iter.next();
                try {
                    curator.setData().forPath(entry.key, entry.value.getBytes());
                } catch (Exception e) {
                }
            }
        }

    }

}
