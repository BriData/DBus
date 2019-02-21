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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.commons.ControlType;
import com.creditease.dbus.commons.DataType;
import com.creditease.dbus.commons.DbusMessage;
import com.creditease.dbus.commons.DbusMessageBuilder;
import com.creditease.dbus.msgencoder.PluggableMessageEncoder;
import com.creditease.dbus.msgencoder.PluginManagerProvider;
import com.creditease.dbus.msgencoder.UmsEncoder;
import com.creditease.dbus.router.base.DBusRouterBase;
import com.creditease.dbus.router.bean.EmitWarp;
import com.creditease.dbus.router.bean.FixColumnOutPutMeta;
import com.creditease.dbus.router.bean.Stat;
import com.creditease.dbus.router.bolt.stat.StatWindows;
import com.creditease.dbus.router.encode.DBusRouterEncodeColumn;
import com.creditease.dbus.router.encode.DBusRouterPluginLoader;
import com.creditease.dbus.router.util.DBusRouterConstants;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
 * Created by mal on 2018/5/22.
 */
public class DBusRouterEncodeBolt extends BaseRichBolt {

    private static Logger logger = LoggerFactory.getLogger(DBusRouterEncodeBolt.class);

    private TopologyContext context = null;
    private OutputCollector collector = null;
    private DBusRouterEncodeBoltInner inner = null;

    /** 表脱敏相关配置缓存 */
    private Map<Long, List<DBusRouterEncodeColumn>> encodeConfigMap = null;
    private Map<Long, Map<String, DBusRouterEncodeColumn>> tableIdColumnNameEncodeCache = new HashMap<>();

    /** 表固定列输出配置缓存 */
    private Map<Long, Long> fixOutTableVersionMap = new HashMap<>();
    private Map<Long, Set<String>> fixOutTableColumnsMap = new HashMap<>();
    private Map<Long, Map<String, FixColumnOutPutMeta>> tableIdColumnNameMetaCache = new HashMap<>();

    /** 统计窗口 */
    private StatWindows statWindows = null;

    private long count = 100L;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        try {
            this.context = context;
            this.collector = collector;
            statWindows = new StatWindows();
            inner = new DBusRouterEncodeBoltInner(conf);
            init(false);
            logger.info("encode bolt init completed.");
        } catch (Exception e) {
            logger.error("encode bolt init error.", e);
            collector.reportError(e);
        }
    }

    @Override
    public void execute(Tuple input) {
        EmitWarp<ConsumerRecord<String, byte[]>> data = (EmitWarp<ConsumerRecord<String, byte[]>>) input.getValueByField("data");
        try {
            if (data.isCtrl()) {
                String ctrl = new String(data.getData().value(), "UTF-8");
                processCtrlMsg(ctrl);
                if (context.getThisTaskIndex() == 0)
                    emitCtrlData(data.getKey(), ctrl, input);
            } else if (data.isHB()) {
                String hb = new String(data.getData().value(), "UTF-8");
                emitStatData(data, input);
                emitData(data.getKey(), data.getTableId(), hb, input);
            } else if (data.isUMS()) {
                String ums = processUmsMsg(data);
                emitData(data.getKey(), data.getTableId(), ums, input);
            } else {
                logger.warn("not support process type. emit warp key: {}", data.getKey());
            }
            collector.ack(input);
        } catch (Exception e) {
            collector.fail(input);
            if (data.isUMS()) statWindows.correc(data.getNameSpace(), data.getSize());
            logger.error("encode bolt execute error data: {}", JSON.toJSONString(data));
            e.printStackTrace();
            logger.error("encode bolt execute error.", e);
            collector.reportError(e);
        }
    }

    @Override
    public void cleanup() {
        destroy();
        inner.close(false);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("umsOrHbStream", new Fields("data"));
        declarer.declareStream("ctrlStream", new Fields("data"));
        declarer.declareStream("statStream", new Fields("data"));
    }

    private void processCtrlMsg(String strCtrl) throws Exception {
        logger.info("encode bolt process ctrl msg start. cmd:{}", strCtrl);
        JSONObject jsonCtrl = JSONObject.parseObject(strCtrl);
        ControlType cmd = ControlType.getCommand(jsonCtrl.getString("type"));
        switch (cmd) {
            case ROUTER_RELOAD:
                reload(strCtrl);
                break;
            case ROUTER_TOPOLOGY_TABLE_EFFECT:
                effectTopologyTable(jsonCtrl);
                break;
            case ROUTER_TOPOLOGY_TABLE_START:
                startTopologyTable(jsonCtrl);
                break;
            case ROUTER_TOPOLOGY_TABLE_STOP:
                stopTopologyTable(jsonCtrl);
                break;
            case ROUTER_TOPOLOGY_RERUN:
                rerunTopology(jsonCtrl);
                break;
            default:
                break;
        }
    }

    private void reload(String ctrl) throws Exception {
        destroy();
        inner.close(true);
        init(true);
        inner.zkHelper.saveReloadStatus(ctrl, "DBusRouterEncodeBolt-" + context.getThisTaskId() , true);
        logger.info("encode bolt reload completed.");
    }

    private void effectTopologyTable(JSONObject ctrl) throws Exception {
        JSONObject payload = ctrl.getJSONObject("payload");
        Integer projectTopologyTableId = payload.getInteger("projectTopoTableId");
        Long tableId = payload.getLong("tableId");
        Map<Long, List<DBusRouterEncodeColumn>> ecMap = inner.dbHelper.loadEncodeConfig(inner.topologyId, projectTopologyTableId);
        if (ecMap != null) {
            for (Map.Entry<Long, List<DBusRouterEncodeColumn>> entry : ecMap.entrySet()) {
                if (this.encodeConfigMap == null) this.encodeConfigMap = new HashMap<>();
                this.encodeConfigMap.put(entry.getKey(), entry.getValue());
                this.tableIdColumnNameEncodeCache.remove(entry.getKey());
                initTableIdColumnNameEncodeCache(ecMap);
            }
        } else {
            if (this.encodeConfigMap != null) this.encodeConfigMap.remove(tableId);
            this.tableIdColumnNameEncodeCache.remove(tableId);
        }
        logger.info("encode config Map: {}", JSON.toJSONString(encodeConfigMap));

        logger.info("fix Out Table Columns Map before: {}", JSON.toJSONString(fixOutTableColumnsMap));
        logger.info("fix Out Table Version Map before: {}", JSON.toJSONString(fixOutTableVersionMap));
        logger.info("table id column name meta cache before: {}", JSON.toJSONString(tableIdColumnNameMetaCache));
        fixOutTableColumnsMap.remove(tableId);
        fixOutTableVersionMap.remove(tableId);
        tableIdColumnNameMetaCache.remove(tableId);
        Map<Long, List<FixColumnOutPutMeta>> metaMap = inner.dbHelper.loadFixColumnOutPutMeta(inner.topologyId, projectTopologyTableId);
        logger.info("fix column out put meta map:{}", JSON.toJSONString(metaMap));
        initFixOutPutInfo(metaMap, true);
        // 重新加载脱敏插件
        PluginManagerProvider.reloadManager();
        logger.info("encode bolt effect topology table:{} completed.", projectTopologyTableId);
    }

    private void startTopologyTable(JSONObject ctrl) throws Exception {
        effectTopologyTable(ctrl);
        statWindows.clear();
        logger.info(" encode bolt start topology table completed.");
    }

    private void stopTopologyTable(JSONObject ctrl) throws Exception {
        JSONObject payload = ctrl.getJSONObject("payload");
        Long tableId = payload.getLong("tableId");
        if (encodeConfigMap != null) encodeConfigMap.remove(tableId);
        tableIdColumnNameEncodeCache.remove(tableId);

        logger.info("fix Out Table Columns Map before: {}", JSON.toJSONString(fixOutTableColumnsMap));
        logger.info("fix Out Table Version Map before: {}", JSON.toJSONString(fixOutTableVersionMap));
        fixOutTableColumnsMap.remove(tableId);
        fixOutTableVersionMap.remove(tableId);
        tableIdColumnNameMetaCache.remove(tableId);
        logger.info("fix Out Table Columns Map after: {}", JSON.toJSONString(fixOutTableColumnsMap));
        logger.info("fix Out Table Version Map after: {}", JSON.toJSONString(fixOutTableVersionMap));
        statWindows.clear();
        logger.info("encode bolt stop topology table completed.");
    }

    private void rerunTopology(JSONObject ctrl) throws Exception {
        statWindows.clear();
        logger.info("encode bolt rerun topology completed.");
    }

    private String processUmsMsg(EmitWarp<ConsumerRecord<String, byte[]>> data) throws Exception {
        DbusMessage ums = obtainUms(data);
        updateSchemaChangeFlag(data.getTableId());
        if (encodeConfigMap != null &&  encodeConfigMap.get(data.getTableId()) != null) {
            UmsEncoder encoder = new PluggableMessageEncoder(PluginManagerProvider.getManager(), (e, column, message) -> {});
            encoder.encode(ums, encodeConfigMap.get(data.getTableId()));
        } else {
            logger.info("table id:{}, name space:{}, 脱敏配置信息为空,因此不执行脱敏.", data.getTableId(), data.getNameSpace());
        }
        return ums.toString();
    }

    private void emitCtrlData(String key, String ctrl, Tuple input) {
        EmitWarp<String> emitData = new EmitWarp<>(key);
        emitData.setData(ctrl);
        this.collector.emit("ctrlStream", input, new Values(emitData));
    }

    private void emitStatData(EmitWarp<ConsumerRecord<String, byte[]>> data, Tuple input) {
        Stat vo = statWindows.poll(data.getNameSpace());
        if (vo == null) {
            collectStat(data, 0);
            vo = statWindows.poll(data.getNameSpace());
        }
        EmitWarp<Stat> emitData = new EmitWarp<>("stat");
        emitData.setNameSpace(data.getNameSpace());
        emitData.setHbTime(data.getHbTime());
        vo.setTime(data.getHbTime());
        vo.setTxTime(data.getHbTime());
        emitData.setData(vo);
        this.collector.emit("statStream", input, new Values(emitData));
    }

    private void emitData(String key, Long tableId, String ums, Tuple input) {
        EmitWarp<String> emitData = new EmitWarp<>(key);
        emitData.setData(ums);
        emitData.setTableId(tableId);
        this.collector.emit("umsOrHbStream", input, new Values(emitData));
    }

    private String obtainNameSapce(String ns, Long tableId) {
        // eg. mysql.caiwudb.fso_yao_db.customer_offline.4.0.0
        // String[] vals = ArrayUtils.insert(4, StringUtils.split(ns, "."), inner.topologyId);
        String[] vals = StringUtils.split(ns, ".");
        vals[1] = StringUtils.joinWith("!", vals[1], inner.topologyId);
        if (fixOutTableVersionMap.get(tableId) == null)
            return StringUtils.joinWith(".", vals);

        // 把namespace的版本号替换成固定列输出版本号
        vals[4] = String.valueOf(fixOutTableVersionMap.get(tableId));
        return StringUtils.joinWith(".", vals);
    }

    private void collectStat(EmitWarp<ConsumerRecord<String, byte[]>> data, Integer cnt) {
        Stat statVo = new Stat();
        statVo.setDsName(data.getDsName());
        statVo.setSchemaName(data.getSchemaName());
        statVo.setTableName(data.getTableName());
        statVo.setSuccessCnt(cnt);
        statVo.setTaskId(context.getThisTaskId());
//        statVo.setTime(data.getHbTime());
        statWindows.add(data.getNameSpace(), statVo);
    }

    private DbusMessage obtainUms(EmitWarp<ConsumerRecord<String, byte[]>> data) throws Exception {
        String strUms = new String(data.getData().value(), "UTF-8");
        JSONObject ums = JSON.parseObject(strUms);
        JSONObject protocol = ums.getJSONObject("protocol");
        JSONObject schema = ums.getJSONObject("schema");
        JSONArray payload = ums.getJSONArray("payload");

        collectStat(data, payload.size());
        // 出错时修正使用
        data.setSize(payload.size());

        DbusMessageBuilder builder = new DbusMessageBuilder(protocol.getString("version"));
        String nameSpace = obtainNameSapce(schema.getString("namespace"), data.getTableId());
        builder.build(obtainProtocolType(protocol.getString("type")), nameSpace, schema.getIntValue("batchId"));

        List<Integer> removeIndex = new ArrayList<>();
        List<Integer> reservedIndex = new ArrayList<>();

        Set<String> columns = fixOutTableColumnsMap.get(data.getTableId());
        JSONArray fields = schema.getJSONArray("fields");
        for (int i=0; i<fields.size(); i++) {
            JSONObject field = fields.getJSONObject(i);
            String name = field.getString("name");

            if ((StringUtils.equalsIgnoreCase(name, DbusMessage.Field._UMS_ID_) ||
                StringUtils.equalsIgnoreCase(name, DbusMessage.Field._UMS_OP_) ||
                StringUtils.equalsIgnoreCase(name, DbusMessage.Field._UMS_TS_) ||
                StringUtils.equalsIgnoreCase(name, DbusMessage.Field._UMS_UID_)) &&
                (i <= 3)) {
                reservedIndex.add(i);
                continue;
            }

            // columns 不等于null 说明采用的是固定列输出
            if (columns != null) {
                if (columns.contains(name)) {
                    reservedIndex.add(i);
                    builder.appendSchema(name,
                                        obtainDataType(field.getString("type")),
                                        field.getBoolean("nullable"),
                                        field.getBoolean("encoded"));

                    FixColumnOutPutMeta fcopm = tableIdColumnNameMetaCache.get(data.getTableId()).get(name);
                    if (fcopm != null) {
                        fcopm.setExist(true);
                        String type = obtainDataTypeFromDbType(fcopm.getDataType(), data.isMysql(), data.isOracle(), fcopm.getPrecision(), fcopm.getScale());
                        if (StringUtils.isNotBlank(type) && !StringUtils.equalsIgnoreCase(type, field.getString("type"))) {
                            fcopm.setChanged(true);
                            String changeComment = fcopm.getDataType() + "->" + field.getString("type");
                            fcopm.setSchemaChangeComment(changeComment);
                            logger.info("meta cache info column:{} {}", fcopm.getColumnName(), changeComment);
                            fcopm.setDataType(field.getString("type"));
                        }
                    }

                    if (tableIdColumnNameEncodeCache.get(data.getTableId()) != null) {
                        DBusRouterEncodeColumn ec = tableIdColumnNameEncodeCache.get(data.getTableId()).get(name);
                        if (ec != null) {
                            ec.setCanEncode(true);
                            String type = obtainDataTypeFromDbType(ec.getFieldType(), data.isMysql(), data.isOracle(), ec.getPrecision(), ec.getScale());
                            if (StringUtils.isNotBlank(type) && !StringUtils.equalsIgnoreCase(type, field.getString("type"))) {
                                ec.setChanged(true);
                                String changeComment = ec.getFieldType() + "->" + field.getString("type");
                                ec.setSchemaChangeComment(changeComment);
                                logger.info("encode cache info column:{} {}", ec.getFieldName(), changeComment);
                                ec.setFieldType(field.getString("type"));
                            }
                        }
                    }

                } else {
                    removeIndex.add(i);
                }
            } else {
                builder.appendSchema(name,
                                    obtainDataType(field.getString("type")),
                                    field.getBoolean("nullable"),
                                    field.getBoolean("encoded"));

                if (tableIdColumnNameEncodeCache.get(data.getTableId()) != null) {
                    DBusRouterEncodeColumn ec = tableIdColumnNameEncodeCache.get(data.getTableId()).get(name);
                    if (ec != null) {
                        ec.setCanEncode(true);
                        String type = obtainDataTypeFromDbType(ec.getFieldType(), data.isMysql(), data.isOracle(), ec.getPrecision(), ec.getScale());
                        if (StringUtils.isNotBlank(type) && !StringUtils.equalsIgnoreCase(type, field.getString("type"))) {
                            ec.setChanged(true);
                            String changeComment = ec.getFieldType() + "->" + field.getString("type");
                            ec.setSchemaChangeComment(changeComment);
                            logger.info("encode cache info column:{} {}", ec.getFieldName(), changeComment);
                            ec.setFieldType(field.getString("type"));
                        }
                    }
                }
            }
        }

        for (int i=0; i<payload.size(); i++) {
            JSONObject tuple = payload.getJSONObject(i);
            JSONArray jsonArrayValue = tuple.getJSONArray("tuple");
            // columns 不等于null 说明采用的是固定列输出
            if (columns != null) {
                List<Object> values = jsonArrayValue.toJavaList(Object.class);
                if (reservedIndex.size() > removeIndex.size()) {
                    List<Object> values_wk = new LinkedList<>(values);
                    for (int idx = removeIndex.size() -1; idx>=0; idx--)
                        values_wk.remove(idx);

                    // 出现删除列或者变更列名找不到的场合,按照原来的列名补齐字段,值补成null
                    for (Map.Entry<String, FixColumnOutPutMeta> entry : tableIdColumnNameMetaCache.get(data.getTableId()).entrySet()) {
                        FixColumnOutPutMeta fcopm = entry.getValue();
                        if (!fcopm.isExist()) {
                            if (i == 0) {
                                String type = obtainDataTypeFromDbType(fcopm.getDataType(), data.isMysql(), data.isOracle(), fcopm.getPrecision(), fcopm.getScale());
                                builder.appendSchema(entry.getKey(), obtainDataType(type), true, false);
                            }
                            values_wk.add(null);
                        }
                    }

                    builder.appendPayload(values_wk.toArray());
                } else {
                    List values_wk = new ArrayList();
                    for (int idx : reservedIndex)
                        values_wk.add(values.get(idx));

                    // 出现删除列或者变更列名找不到的场合,按照原来的列名补齐字段,值补成null
                    for (Map.Entry<String, FixColumnOutPutMeta> entry : tableIdColumnNameMetaCache.get(data.getTableId()).entrySet()) {
                        FixColumnOutPutMeta fcopm = entry.getValue();
                        if (!fcopm.isExist()) {
                            if (i == 0) {
                                String type = obtainDataTypeFromDbType(fcopm.getDataType(), data.isMysql(), data.isOracle(), fcopm.getPrecision(), fcopm.getScale());
                                builder.appendSchema(entry.getKey(), obtainDataType(type), true, false);
                            }
                            values_wk.add(null);
                        }
                    }

                    builder.appendPayload(values_wk.toArray());
                }
            } else {
                builder.appendPayload(jsonArrayValue.toArray());
            }
        }

        return builder.getMessage();
    }

    private void updateSchemaChangeFlag(Long tableId) throws Exception {
        boolean isUpdateTptt = false;
        if (tableIdColumnNameMetaCache.get(tableId) != null) {
            for (Map.Entry<String, FixColumnOutPutMeta> entry : tableIdColumnNameMetaCache.get(tableId).entrySet()) {
                FixColumnOutPutMeta fcopm = entry.getValue();
                if (fcopm.isChanged()) {
                    if (fcopm.getSchemaChangeFlag() == DBusRouterConstants.SCHEMA_CHANGE_COLUMN_NONE) {
                        inner.dbHelper.updateTpttmvSchemaChange(fcopm.getTpttmvId(),
                                DBusRouterConstants.SCHEMA_CHANGE_COLUMN_TYPE_UPDATE,
                                fcopm.getSchemaChangeComment());
                        logger.info("tpttmv schema changed id:{}, column:{}", fcopm.getTpttmvId(), fcopm.getColumnName());
                        if (!isUpdateTptt) {
                            inner.dbHelper.updateTpttSchemaChange(fcopm.getTpttId(),
                                    DBusRouterConstants.SCHEMA_CHANGE_TABLE_UPDATE);
                            logger.info("tptt schema changed id:{}", fcopm.getTpttId());
                            isUpdateTptt = true;
                        }
                    } else {
                        fcopm.setSchemaChangeFlag(DBusRouterConstants.SCHEMA_CHANGE_COLUMN_NONE);
                    }
                }
                if (!fcopm.isExist()) {
                    fcopm.setDelete(true);
                    if (fcopm.getSchemaChangeFlag() == DBusRouterConstants.SCHEMA_CHANGE_COLUMN_NONE) {
                        fcopm.setSchemaChangeFlag(DBusRouterConstants.SCHEMA_CHANGE_COLUMN_DELETE);
                        inner.dbHelper.updateTpttmvSchemaChange(fcopm.getTpttmvId(),
                                                                DBusRouterConstants.SCHEMA_CHANGE_COLUMN_DELETE,
                                                                "deleted");
                        logger.info("tpttmv schema changed id:{}, column:{} deleted", fcopm.getTpttmvId(), fcopm.getColumnName());
                        if (!isUpdateTptt) {
                            inner.dbHelper.updateTpttSchemaChange(fcopm.getTpttId(),
                                                                  DBusRouterConstants.SCHEMA_CHANGE_TABLE_UPDATE);
                            logger.info("tptt schema changed id:{}", fcopm.getTpttId());
                            isUpdateTptt = true;
                        }
                    }
                } else if (fcopm.isExist() && fcopm.isDelete()) {
                    fcopm.setDelete(false);
                    fcopm.setSchemaChangeFlag(DBusRouterConstants.SCHEMA_CHANGE_COLUMN_NONE);
                    inner.dbHelper.updateTpttmvSchemaChange(fcopm.getTpttmvId(),
                                                            DBusRouterConstants.SCHEMA_CHANGE_COLUMN_DELETE,
                                                            "added");
                    logger.info("tpttmv schema changed id:{}, column:{} added", fcopm.getTpttmvId(), fcopm.getColumnName());
                }
                fcopm.setChanged(false);
                fcopm.setExist(false);
            }
        }
        if (tableIdColumnNameEncodeCache.get(tableId) != null) {
            for (Map.Entry<String, DBusRouterEncodeColumn> entry : tableIdColumnNameEncodeCache.get(tableId).entrySet()) {
                DBusRouterEncodeColumn ec = entry.getValue();
                if (ec.isChanged()) {
                    if (ec.getSchemaChangeFlag() == DBusRouterConstants.SCHEMA_CHANGE_COLUMN_NONE) {
                        inner.dbHelper.updateTptteocSchemaChange(ec.getTptteocId(),
                                DBusRouterConstants.SCHEMA_CHANGE_COLUMN_TYPE_UPDATE,
                                ec.getSchemaChangeComment());
                        logger.info("tptteoc schema changed id:{}, column:{}", ec.getTptteocId(), ec.getFieldName());
                        if (!isUpdateTptt) {
                            inner.dbHelper.updateTpttSchemaChange(ec.getTpttId(),
                                    DBusRouterConstants.SCHEMA_CHANGE_TABLE_UPDATE);
                            logger.info("tptt schema changed id:{}", ec.getTpttId());
                            isUpdateTptt = true;
                        }
                    } else {
                        ec.setSchemaChangeFlag(DBusRouterConstants.SCHEMA_CHANGE_COLUMN_NONE);
                    }
                }
                if (!ec.isCanEncode()) {
                    ec.setDelete(true);
                    if (ec.getSchemaChangeFlag() == DBusRouterConstants.SCHEMA_CHANGE_COLUMN_NONE) {
                        ec.setSchemaChangeFlag(DBusRouterConstants.SCHEMA_CHANGE_COLUMN_DELETE);
                        inner.dbHelper.updateTptteocSchemaChange(ec.getTptteocId(),
                                                                 DBusRouterConstants.SCHEMA_CHANGE_COLUMN_DELETE,
                                                                 "deleted");
                        logger.info("tptteoc schema changed id:{}, column:{} deleted", ec.getTptteocId(), ec.getFieldName());
                        if (!isUpdateTptt) {
                            inner.dbHelper.updateTpttSchemaChange(ec.getTpttId(),
                                                                  DBusRouterConstants.SCHEMA_CHANGE_TABLE_UPDATE);
                            logger.info("tptt schema changed id:{}", ec.getTpttId());
                            isUpdateTptt = true;
                        }
                    }
                } else if (ec.isCanEncode() && ec.isDelete()) {
                    ec.setDelete(false);
                    ec.setSchemaChangeFlag(DBusRouterConstants.SCHEMA_CHANGE_COLUMN_NONE);
                    inner.dbHelper.updateTptteocSchemaChange(ec.getTptteocId(),
                                                             DBusRouterConstants.SCHEMA_CHANGE_COLUMN_DELETE,
                                                             "added");
                    logger.info("tptteoc schema changed id:{}, column:{} added", ec.getTptteocId(), ec.getFieldName());
                }
                ec.setChanged(false);
                ec.setCanEncode(false);
            }
        }
    }

    private DbusMessage.ProtocolType obtainProtocolType(String protocol) {
        if (StringUtils.equalsIgnoreCase(protocol, DbusMessage.ProtocolType.DATA_INCREMENT_DATA.toString()))
            return DbusMessage.ProtocolType.DATA_INCREMENT_DATA;
        if (StringUtils.equalsIgnoreCase(protocol, DbusMessage.ProtocolType.DATA_INITIAL_DATA.toString()))
            return DbusMessage.ProtocolType.DATA_INITIAL_DATA;
        if (StringUtils.equalsIgnoreCase(protocol, DbusMessage.ProtocolType.DATA_INCREMENT_HEARTBEAT.toString()))
            return DbusMessage.ProtocolType.DATA_INCREMENT_HEARTBEAT;
        return DbusMessage.ProtocolType.DATA_INCREMENT_TERMINATION;
    }

    public String obtainDataTypeFromDbType(String type, boolean isMysql, boolean isOracle, int precision, int scale) {
        String ret = "";
        if (isMysql) {
            ret = DataType.convertMysqlDataType(type).toString();
        } else if (isOracle) {
            ret = DataType.convert(type, precision, scale).toString();
        }
        return ret;
    }

    public DataType obtainDataType(String type) {
        if (StringUtils.equalsIgnoreCase(type, DataType.INT.toString()))
            return DataType.INT;
        if (StringUtils.equalsIgnoreCase(type, DataType.LONG.toString()))
            return DataType.LONG;
        if (StringUtils.equalsIgnoreCase(type, DataType.FLOAT.toString()))
            return DataType.FLOAT;
        if (StringUtils.equalsIgnoreCase(type, DataType.DOUBLE.toString()))
            return DataType.DOUBLE;
        if (StringUtils.equalsIgnoreCase(type, DataType.BOOLEAN.toString()))
            return DataType.BOOLEAN;
        if (StringUtils.equalsIgnoreCase(type, DataType.DATE.toString()))
            return DataType.DATE;
        if (StringUtils.equalsIgnoreCase(type, DataType.DATETIME.toString()))
            return DataType.DATETIME;
        if (StringUtils.equalsIgnoreCase(type, DataType.DECIMAL.toString()))
            return DataType.DECIMAL;
        if (StringUtils.equalsIgnoreCase(type, DataType.BINARY.toString()))
            return DataType.BINARY;
        if (StringUtils.equalsIgnoreCase(type, DataType.RAW.toString()))
            return DataType.RAW;
        return DataType.STRING;
    }

    private void destroy() {
        if (encodeConfigMap != null) encodeConfigMap.clear();
        fixOutTableColumnsMap.clear();
        fixOutTableVersionMap.clear();
    }

    private void init(boolean isReload) throws Exception {
        inner.init();
        this.encodeConfigMap = inner.dbHelper.loadEncodeConfig(inner.topologyId);
        logger.info("encode config Map: {}", JSON.toJSONString(encodeConfigMap));
        initTableIdColumnNameEncodeCache(this.encodeConfigMap);

        Map<Long, List<FixColumnOutPutMeta>> metaMap = inner.dbHelper.loadFixColumnOutPutMeta(inner.topologyId);
        initFixOutPutInfo(metaMap, false);
        if (!isReload) {
            DBusRouterPluginLoader loader = new DBusRouterPluginLoader(inner.dbHelper, inner.topologyId);
            PluginManagerProvider.initialize(loader);
        } else {
            PluginManagerProvider.reloadManager();
        }
    }

    private void initTableIdColumnNameEncodeCache(Map<Long, List<DBusRouterEncodeColumn>> encodeMap) {
        if (encodeMap != null) {
            for (Map.Entry<Long, List<DBusRouterEncodeColumn>> entry : encodeMap.entrySet()) {
                for (DBusRouterEncodeColumn ec : entry.getValue()) {
                    if (!tableIdColumnNameEncodeCache.containsKey(entry.getKey())) {
                        Map<String, DBusRouterEncodeColumn> map = new HashMap<>();
                        map.put(ec.getFieldName(), ec);
                        tableIdColumnNameEncodeCache.put(entry.getKey(), map);
                    } else {
                        tableIdColumnNameEncodeCache.get(entry.getKey()).put(ec.getFieldName(), ec);
                    }
                }
            }
            logger.info("table id column name encode cache: {}", JSON.toJSONString(tableIdColumnNameEncodeCache));
        }
    }

    private void initFixOutPutInfo(Map<Long, List<FixColumnOutPutMeta>> metaMap, boolean isCtrl) {
        if (metaMap != null) {
            for (Map.Entry<Long, List<FixColumnOutPutMeta>> entry : metaMap.entrySet()) {
                for (FixColumnOutPutMeta vo : entry.getValue()) {
                    if (!fixOutTableColumnsMap.containsKey(entry.getKey())) {
                        Set<String> columns = new HashSet<>();
                        columns.add(vo.getColumnName());
                        fixOutTableColumnsMap.put(entry.getKey(), columns);
                    } else {
                        fixOutTableColumnsMap.get(entry.getKey()).add(vo.getColumnName());
                    }

                    //
                    if (!fixOutTableVersionMap.containsKey(entry.getKey()))
                        fixOutTableVersionMap.put(entry.getKey(), vo.getVersion());

                    if (!tableIdColumnNameMetaCache.containsKey(entry.getKey())) {
                        Map<String, FixColumnOutPutMeta> map = new HashMap<>();
                        map.put(vo.getColumnName(), vo);
                        tableIdColumnNameMetaCache.put(entry.getKey(), map);
                    } else {
                        tableIdColumnNameMetaCache.get(entry.getKey()).put(vo.getColumnName(), vo);
                    }

                }
            }
        }
        logger.info("fix Out Table Columns Map after: {}", JSON.toJSONString(fixOutTableColumnsMap));
        logger.info("fix Out Table Version Map after: {}", JSON.toJSONString(fixOutTableVersionMap));
        logger.info("table id column name meta cache after: {}", JSON.toJSONString(tableIdColumnNameMetaCache));
    }

    private class DBusRouterEncodeBoltInner extends DBusRouterBase {
        public DBusRouterEncodeBoltInner(Map conf) {
            super(conf);
        }
    }

}
