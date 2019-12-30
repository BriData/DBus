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


package com.creditease.dbus.log.processor.bolt;

import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.commons.ControlType;
import com.creditease.dbus.log.processor.base.LogProcessorBase;
import com.creditease.dbus.log.processor.util.Constants;
import com.creditease.dbus.log.processor.window.Element;
import com.creditease.dbus.log.processor.window.LogProcessorWindow;
import com.creditease.dbus.log.processor.window.StatWindowInfo;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;


public class LogProcessorStatBolt extends BaseRichBolt {

    private static Logger logger = LoggerFactory.getLogger(LogProcessorStatBolt.class);

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
        logger.info("LogProcessorStatBolt is started!");
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
            String outputTopic = inner.logProcessorConf.getProperty("dbus.statistic.topic");
            switch (emitDataType) {
                case Constants.EMIT_DATA_TYPE_CTRL:
                    String json = (String) input.getValueByField("value");
                    processControlCommand(json, input);
                    break;
                case Constants.EMIT_DATA_TYPE_NORMAL:
                    StatWindowInfo swi = (StatWindowInfo) input.getValueByField("value");
                    logProcessorWindow.offer(swi);
                    List<Element> swiList = logProcessorWindow.deliver();
                    for (Element e : swiList) {
                        swi = (StatWindowInfo) e;
                        collector.emit(input, new Values(outputTopic, swi, Constants.EMIT_DATA_TYPE_STAT));
                    }
                    break;
                default:
                    break;
            }
            collector.ack(input);
        } catch (Exception e) {
            logger.error("LogProcessorStatBolt execute error:", e);
            collector.fail(input);
            collector.reportError(e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("outputTopic", "value", "emitDataType"));
    }

    @Override
    public void cleanup() {
        inner.close(false);
    }

    private void processControlCommand(String json, Tuple input) {
        try {
            ControlType cmd = ControlType.getCommand(JSONObject.parseObject(json).getString("type"));
            switch (cmd) {
                case LOG_PROCESSOR_RELOAD_CONFIG: {
                    logger.info("LogProcessorStatBolt-{} 收到reload消息！Type: {}, Values: {} ", context.getThisTaskId(), cmd, json);
                    inner.close(true);
                    init();
                    inner.zkHelper.saveReloadStatus(json, "LogProcessorStatBolt-" + context.getThisTaskId(), true);
                    logger.info("LogProcessorStatBolt reload success!");
                    break;
                }
                default:
                    break;
            }

        } catch (Exception e) {
            logger.error("LogProcessorTransformBolt processControlCommand():", e);
            collector.reportError(e);
            collector.fail(input);
        }
    }

    private class LogProcessorKafkaWriteBoltInner extends LogProcessorBase {
        public LogProcessorKafkaWriteBoltInner(Map conf) {
            super(conf);
        }
    }

}
