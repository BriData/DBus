


package com.creditease.dbus.log.processor.bolt;

import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.commons.ControlType;
import com.creditease.dbus.commons.DbusMessage;
import com.creditease.dbus.commons.DbusMessageBuilder;
import com.creditease.dbus.enums.DbusDatasourceType;
import com.creditease.dbus.log.processor.base.LogProcessorBase;
import com.creditease.dbus.log.processor.util.Constants;
import com.creditease.dbus.log.processor.util.DateUtil;
import com.creditease.dbus.log.processor.window.Element;
import com.creditease.dbus.log.processor.window.HeartBeatWindowInfo;
import com.creditease.dbus.log.processor.window.LogProcessorWindow;
import com.google.common.base.Joiner;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class LogProcessorHeartbeatBolt extends BaseRichBolt {

    private static Logger logger = LoggerFactory.getLogger(LogProcessorHeartbeatBolt.class);

    private OutputCollector collector = null;
    private LogProcessorKafkaWriteBoltInner inner = null;
    private TopologyContext context = null;
    private LogProcessorWindow logProcessorWindow = null;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.context = context;
        inner = new LogProcessorKafkaWriteBoltInner(conf);
        init();
        List<Integer> taskIds = context.getComponentTasks("LogProcessorTransformBolt");
        logProcessorWindow = new LogProcessorWindow(10000, computeTaskIdsSum(taskIds));
        logger.info("LogProcessorHeartbeatBolt is started!");
    }

    private void init() {
        inner.loadConf();
    }

    private Integer computeTaskIdsSum(List<Integer> taskIds) {
        Integer sum = 0;
        for (Integer taskId : taskIds) {
            sum += taskId;
        }
        return sum;
    }

    @Override
    public void execute(Tuple input) {
        try {
            String emitDataType = (String) input.getValueByField("emitDataType");
            switch (emitDataType) {
                case Constants.EMIT_DATA_TYPE_CTRL:
                    String json = (String) input.getValueByField("value");
                    processControlCommand(json, input);
                    break;
                case Constants.EMIT_DATA_TYPE_HEARTBEAT:
                    HeartBeatWindowInfo hbwi = (HeartBeatWindowInfo) input.getValueByField("value");
                    logProcessorWindow.offer(hbwi);
                    List<Element> swiList = logProcessorWindow.deliver();
                    for (Element e : swiList) {
                        hbwi = (HeartBeatWindowInfo) e;
                        hbwi.setDbusMessage(buildUms(hbwi));
                        collector.emit("heartbeatStream", input, new Values(hbwi.getOutputTopic(), hbwi, Constants.EMIT_DATA_TYPE_HEARTBEAT));
                    }
                    break;
                case Constants.EMIT_DATA_TYPE_NORMAL: {
                    collector.emit("umsStream", input, input.getValues());
                    break;
                }
                default:
                    break;
            }
            collector.ack(input);
        } catch (Exception e) {
            logger.error("LogProcessorHeartbeatBolt execute error:", e);
            collector.fail(input);
            collector.reportError(e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("heartbeatStream", new Fields("outputTopic", "value", "emitDataType"));
        declarer.declareStream("umsStream", new Fields("outputTopic", "table", "value", "emitDataType"));
    }

    @Override
    public void cleanup() {
        inner.close(false);
    }


    private void processControlCommand(String json, Tuple input) {
        try {
            ControlType cmd = ControlType.getCommand(JSONObject.parseObject(json).getString("type"));
            switch (cmd) {
                case LOG_PROCESSOR_RELOAD_CONFIG:
                    logger.info("LogProcessorHeartbeatBolt-{} 收到reload消息！Type: {}, Values: {} ", context.getThisTaskId(), cmd, json);
                    inner.close(true);
                    init();
                    inner.zkHelper.saveReloadStatus(json, "LogProcessorHeartbeatBolt-" + context.getThisTaskId(), true);
                    break;
                default:
                    break;
            }

        } catch (Exception e) {
            logger.error("LogProcessorTransformBolt processControlCommand():", e);
            collector.reportError(e);
            collector.fail(input);
        }
    }

    private DbusMessage buildUms(HeartBeatWindowInfo hbwi) throws Exception {
        String[] vals = StringUtils.split(hbwi.getNamespace(), "|");
        DbusMessageBuilder builder = new DbusMessageBuilder();
        String host = null;
        if (DbusDatasourceType.stringEqual(inner.logProcessorConf.getProperty("log.type"), DbusDatasourceType.LOG_UMS)) {
            host = hbwi.getUmsSource();
        } else {
            host = StringUtils.replaceChars(hbwi.getHost(), ".", "_");
        }
        String namespace = buildNameSpace(vals[0], vals[1], vals[2], Long.valueOf(vals[3]).intValue(), host);
        builder.build(DbusMessage.ProtocolType.DATA_INCREMENT_HEARTBEAT, namespace, 0);
        // 设置schema和payload
        /**
         * 1.UMS_TS : 即@timestamp。
         *
         * 2.UMS_ID:
         * 64位 = 1位符号 + 41位毫秒数 + 4位partition + 18位offset mod
         * 毫秒数为 当前毫秒 – 1483200000000（2017-1-1的毫秒数据），就是意味系统支持时间最大为2086/9/7 15:47:35
         * Partition支持16个partition Offset 最多262143（每毫秒每个partiion最多支持262143条数据）
         *
         */
        Long timeStamp = hbwi.getTimestamp();
        List<Object> values = new ArrayList<>();
        Long ums_id = (timeStamp - 1483200000000L) << 22 | (hbwi.getPartition() << 18) | (hbwi.getOffset() % 262144);
        values.add(ums_id);
        values.add(DateUtil.convertLongToStr4Date(timeStamp));
        builder.appendPayload(values.toArray());
        return builder.getMessage();
    }

    private String buildNameSpace(String datasourceNs, String dbSchema, String table, int ver, String host) {
        return Joiner.on(".").join(datasourceNs, dbSchema, table, ver, host, "0");
    }

    private class LogProcessorKafkaWriteBoltInner extends LogProcessorBase {
        public LogProcessorKafkaWriteBoltInner(Map conf) {
            super(conf);
        }
    }

}
