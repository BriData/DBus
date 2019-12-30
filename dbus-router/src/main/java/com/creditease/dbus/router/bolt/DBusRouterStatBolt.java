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


package com.creditease.dbus.router.bolt;

import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.commons.ControlType;
import com.creditease.dbus.commons.StatMessage;
import com.creditease.dbus.router.base.DBusRouterBase;
import com.creditease.dbus.router.bean.EmitWarp;
import com.creditease.dbus.router.bean.Stat;
import com.creditease.dbus.router.bolt.stat.StatWindows;
import com.creditease.dbus.router.cache.Cache;
import com.creditease.dbus.router.cache.impl.PerpetualCache;
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

import java.util.List;
import java.util.Map;

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

    private Cache cache = null;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        try {
            this.context = context;
            this.collector = collector;
            this.statWindows = new StatWindows(true);
            this.encodeBoltTaskIdSum = computeTaskIdsSum(context.getComponentTasks("RouterEncodeBolt"));
            this.inner = new DBusRouterStatBoltInner(conf);
            this.cache = new PerpetualCache("stat_cache");
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
                // 由于表采用的是分组分发,所有表的数据都到同一个encode bolt,所以在这里不需要进行汇总计算
                // 拿到统计信息后直接发送就可以了,如果进行汇总在多个encode bolt的情况就出现没有统计的错误
                /*Stat statVo = statWindows.tryPoll(ns, encodeBoltTaskIdSum);
                if (statVo != null) {
                    logger.info("encodeBoltTaskIdSum: {}, stat vo: {}", encodeBoltTaskIdSum, JSON.toJSONString(statVo));
                    String stat = obtainStatMessage(statVo);
                    logger.info("emit stat data: {}", stat);
                    emitStatData(stat, ns, input);
                }*/
                String stat = obtainStatMessage((Stat) data.getData());
                emitStatData(stat, ns, input, data.getOffset());
            }
            logger.info("stat bolt ack {}", data.getOffset());
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

    private void emitStatData(String data, String ns, Tuple input, long offset) {
        EmitWarp<String> emitData = new EmitWarp<>("stat");
        emitData.setNameSpace(ns);
        emitData.setData(data);
        emitData.setOffset(offset);
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
                cache.clear();
                inner.dbHelper.loadAliasMapping(inner.topologyId, cache);
                logger.info("stat bolt start/stop topology table completed.");
                break;
            case ROUTER_TOPOLOGY_RERUN:
                statWindows.clear();
                cache.clear();
                inner.dbHelper.loadAliasMapping(inner.topologyId, cache);
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
        inner.zkHelper.saveReloadStatus(ctrl, "DBusRouterStatBolt-" + context.getThisTaskId(), true);
        logger.info("stat bolt reload completed.");
    }

    private void init() throws Exception {
        inner.init();
        cache.clear();
        inner.dbHelper.loadAliasMapping(inner.topologyId, cache);
    }

    private void destroy() {

    }

    private String obtainStatMessage(Stat statVo) {
        String type = StringUtils.joinWith("_", "ROUTER_TYPE", inner.topologyId);
        String dsName = statVo.getDsName();
        if (StringUtils.isNotBlank((String) cache.getObject(dsName))) {
            dsName = (String) cache.getObject(dsName);
        }
        StatMessage sm = new StatMessage(dsName, statVo.getSchemaName(), statVo.getTableName(), type);
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
