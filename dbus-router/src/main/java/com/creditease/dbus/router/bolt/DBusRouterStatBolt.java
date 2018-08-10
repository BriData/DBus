/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2018 Bridata
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

package com.creditease.dbus.router.bolt;

import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.commons.ControlType;
import com.creditease.dbus.commons.StatMessage;
import com.creditease.dbus.router.base.DBusRouterBase;
import com.creditease.dbus.router.bolt.stat.StatWindows;
import com.creditease.dbus.router.bean.EmitWarp;
import com.creditease.dbus.router.bean.Stat;

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

/**
 * Created by mal on 2018/6/6.
 */
public class DBusRouterStatBolt extends BaseRichBolt {

    private static Logger logger = LoggerFactory.getLogger(DBusRouterStatBolt.class);

    private TopologyContext context = null;
    private OutputCollector collector = null;
    private StatWindows statWindows = null;
    private Integer encodeBoltTaskIdSum = -1;
    private DBusRouterStatBoltInner inner = null;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        try {
            this.context = context;
            this.collector = collector;
            this.statWindows = new StatWindows(true);
            this.encodeBoltTaskIdSum = computeTaskIdsSum(context.getComponentTasks("RouterEncodeBolt"));
            this.inner = new DBusRouterStatBoltInner(conf);
            init();
            logger.info("stat bolt init completed.");
        } catch (Exception e) {
            logger.error("stat bolt init error.", e);
            collector.reportError(e);
        }
    }

    @Override
    public void execute(Tuple input) {
        try {
            EmitWarp<?> data = (EmitWarp<?>) input.getValueByField("data");
            if (data.isCtrl()) {
                String ctrl = (String) data.getData();
                processCtrlMsg(ctrl);
            } else if (data.isStat()) {
                String ns = StringUtils.joinWith(".", data.getNameSpace(), data.getHbTime());
                statWindows.add(ns, (Stat) data.getData());
                Stat statVo = statWindows.tryPoll(ns, encodeBoltTaskIdSum);
                if (statVo != null) {
                    String stat = obtainStatMessage(statVo);
                    emitStatData(stat, ns, input);
                }
            }
            collector.ack(input);
        } catch (Exception e) {
            collector.fail(input);
            logger.error("stat bolt execute error.", e);
            collector.reportError(e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("statStream", new Fields("data"));
    }

    @Override
    public void cleanup() {
        super.cleanup();
        destroy();
        inner.close(false);
    }

    private void emitStatData(String data, String ns, Tuple input) {
        EmitWarp<String> emitData = new EmitWarp<>("stat");
        emitData.setNameSpace(ns);
        emitData.setData(data);
        this.collector.emit("statStream", input, new Values(emitData));
    }

    private void processCtrlMsg(String strCtrl) throws Exception {
        logger.info("stat bolt process ctrl msg. cmd:{}", strCtrl);
        JSONObject jsonCtrl = JSONObject.parseObject(strCtrl);
        ControlType cmd = ControlType.getCommand(jsonCtrl.getString("type"));
        switch (cmd) {
            case ROUTER_RELOAD:
                reload(strCtrl);
                break;
            case ROUTER_TOPOLOGY_TABLE_START:
            case ROUTER_TOPOLOGY_TABLE_STOP:
                statWindows.clear();
                logger.info("stat bolt start/stop topology table completed.");
                break;
            case ROUTER_TOPOLOGY_RERUN:
                statWindows.clear();
                logger.info("stat bolt rerun topology table completed.");
                break;
            default:
                break;
        }
    }

    private void reload(String ctrl) throws Exception {
        destroy();
        inner.close(true);
        init();
        inner.zkHelper.saveReloadStatus(ctrl, "DBusRouterStatBolt-" + context.getThisTaskId() , true);
        logger.info("stat bolt reload completed.");
    }

    private void init() throws Exception {
        inner.init();
    }

    private void destroy() {

    }

    private String obtainStatMessage(Stat statVo) {
        String type = StringUtils.joinWith("_", "ROUTER_TYPE", inner.topologyId);
        StatMessage sm = new StatMessage(statVo.getDsName(), statVo.getSchemaName(), statVo.getTableName(), type);
        Long curTime = System.currentTimeMillis();
        sm.setCheckpointMS(statVo.getTime());
        sm.setTxTimeMS(statVo.getTxTime());
        sm.setLocalMS(curTime);
        sm.setLatencyMS(curTime - statVo.getTime());
        sm.setCount(statVo.getSuccessCnt());
        sm.setErrorCount(statVo.getErrorCnt());
        return sm.toJSONString();
    }

    private Integer computeTaskIdsSum(List<Integer> taskIds) {
        Integer sum = 0;
        for (Integer taskId : taskIds) {
            sum += taskId;
        }
        return sum;
    }

    private class DBusRouterStatBoltInner extends DBusRouterBase {
        public DBusRouterStatBoltInner(Map conf) {
            super(conf);
        }
    }

}
