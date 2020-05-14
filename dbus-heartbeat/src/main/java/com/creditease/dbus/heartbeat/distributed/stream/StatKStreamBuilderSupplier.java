package com.creditease.dbus.heartbeat.distributed.stream;

import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;

import com.creditease.dbus.enums.DbusDatasourceType;
import com.creditease.dbus.heartbeat.container.HeartBeatConfigContainer;
import com.creditease.dbus.heartbeat.distributed.stream.processor.MonitorProcessorSupplier;
import com.creditease.dbus.heartbeat.distributed.stream.transformer.StatTransformer;
import com.creditease.dbus.heartbeat.vo.PacketVo;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.TimeWindows;

public class StatKStreamBuilderSupplier implements Supplier<KStreamBuilder> {

    private List<String> sources;
    private String sink;

    public StatKStreamBuilderSupplier(List<String> sources, String sink) {
        this.sources = sources;
        this.sink = sink;
    }

    @Override
    public KStreamBuilder get() {
        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, String> stream = builder.stream((String[]) sources.toArray());

        KStream<HBKeySupplier.HBKey, String>[] streams =
                stream.filter((k, v) -> StringUtils.startsWith(v, "data_increment_heartbeat"))
                        .selectKey((k, v) -> new HBKeySupplier(k).get())
                        .filter((k, v) -> k.isNormalFormat)
                        .flatMapValues(v -> Arrays.asList("stat", "monitor"))
                        .branch((k, v) -> StringUtils.equalsIgnoreCase("stat", v),
                                (k, v) -> StringUtils.equalsIgnoreCase("monitor", v));

        streams[0].transform(StatTransformer::new).to(sink);
        KStream<String, PacketVo> monitor =
                streams[1].filterNot((k, v) -> !StringUtils.equalsIgnoreCase("abort", k.getStatus()))
                        .map((k, v) -> {
                            StringJoiner joiner = new StringJoiner("/");
                            joiner.add(HeartBeatConfigContainer.getInstance().getHbConf().getMonitorPath())
                                    .add(k.getDs())
                                    .add(StringUtils.equalsIgnoreCase(DbusDatasourceType.DB2.name(), k.getDbType()) ? StringUtils.upperCase(k.getSchema()) : k.getSchema())
                                    .add(k.getTable()).add(String.valueOf(k.getPartition()));

                            String node = joiner.toString();
                            PacketVo packet = new PacketVo();
                            packet.setNode(node);
                            packet.setType("checkpoint");
                            packet.setTime(k.getCheckpointMs());
                            packet.setTxTime(k.getTxTimeMs());
                            return new KeyValue(node, packet);
                        });

        // 需要先进行shuff把key相同的分配到partition号
        monitor.through("monitor-repartition")
                .reduceByKey((agg, v) -> v.getTime() > agg.getTime() ? v : agg, TimeWindows.of("monitor", 2 * 60 * 1000))
                .toStream()
                .map((k, v) -> new KeyValue<>(k.key(), v))
                .process(new MonitorProcessorSupplier(), "zkInfo");

        return builder;
    }

}
