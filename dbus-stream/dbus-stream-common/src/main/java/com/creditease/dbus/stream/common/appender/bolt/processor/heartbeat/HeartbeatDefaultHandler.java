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


package com.creditease.dbus.stream.common.appender.bolt.processor.heartbeat;

import avro.shaded.com.google.common.base.Joiner;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.commons.DbusMessage;
import com.creditease.dbus.commons.DbusMessageBuilder;
import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.appender.bean.DataTable;
import com.creditease.dbus.stream.common.appender.bean.EmitData;
import com.creditease.dbus.stream.common.appender.bean.MetaVersion;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandler;
import com.creditease.dbus.stream.common.appender.bolt.processor.listener.HeartbeatHandlerListener;
import com.creditease.dbus.stream.common.appender.utils.PairWrapper;
import com.creditease.dbus.stream.common.appender.utils.Utils;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 在重新加载bolt时不能重新创建这个类的实例,否则会无法控制重新加载次数
 * Created by Shrimp on 16/7/4.
 */
public class HeartbeatDefaultHandler implements BoltCommandHandler {

    private Logger logger = LoggerFactory.getLogger(getClass());

    //private static final String HEARTBEAT_FLAG = "heartbeat";
    private HeartbeatHandlerListener listener;

    public HeartbeatDefaultHandler(HeartbeatHandlerListener listener) {
        this.listener = listener;
    }

    @Override
    public void handle(Tuple tuple) {
        EmitData data = (EmitData) tuple.getValueByField(Constants.EmitFields.DATA);
        List<PairWrapper<String, Object>> wrapperList = data.get(EmitData.MESSAGE);
        DataTable table = data.get(EmitData.DATA_TABLE);
        MetaVersion ver = data.get(EmitData.VERSION);
        for (PairWrapper<String, Object> wrapper : wrapperList) {
            Object packet = wrapper.getPairValue("PACKET");
            if (packet == null) {
                logger.warn("[appender-heartbeat] data error. wrapper:{}", JSON.toJSONString(wrapper));
                continue;
            }
            JSONObject json = JSON.parseObject(packet.toString());
            String dbSchema = wrapper.getPairValue("SCHEMA_NAME").toString();
            String tableName = wrapper.getPairValue("TABLE_NAME").toString();

            String pos = generateUmsId(wrapper.getProperties(Constants.MessageBodyKey.POS).toString());

            // oracle 数据格式：2017-03-24 14:28:00.995660，mysql格式：2017-03-24 14:28:00.995
            // 暂时不要统一
            //String opts = wrapper.getProperties(Constants.MessageBodyKey.OP_TS).toString().substring(0, 23);
            String opts = wrapper.getProperties(Constants.MessageBodyKey.OP_TS).toString();
            long time = json.getLong("time");
            long txTime = json.getLong("txTime");

            String targetTopic = listener.getTargetTopic(dbSchema, tableName);
            // 发送Heartbeat
            listener.sendHeartbeat(message(table, ver, pos, opts), targetTopic, buildKey(time + "|" + txTime, table, ver));
        }
    }

    protected String generateUmsId(String pos) {
        return pos;
    }

    private DbusMessage message(DataTable table, MetaVersion ver, String pos, String opts) {
        DbusMessageBuilder builder = new DbusMessageBuilder();
        return builder.build(DbusMessage.ProtocolType.DATA_INCREMENT_HEARTBEAT, buildNs(table, ver), table.getBatchId())
                .appendPayload(new Object[]{pos, opts})
                .getMessage();
    }

    private String buildKey(String timeStr, DataTable table, MetaVersion ver) {
        return Utils.join(".", DbusMessage.ProtocolType.DATA_INCREMENT_HEARTBEAT.toString(),
                buildNs(table, ver), timeStr + "|" + table.getStatus() + "|" + Utils.getNameAliasMapping().getName(), "wh_placeholder");
    }

    private String buildNs(DataTable table, MetaVersion ver) {
        return Joiner.on(".").join(Utils.getDataSourceNamespace(), table.getSchema(), table.getTableName(),
                ver.getVersion(), 0, table.isPartationTable() ? "*" : 0);
    }
}
