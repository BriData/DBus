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
import com.creditease.dbus.commons.DataType;
import com.creditease.dbus.commons.DbusMessage;
import com.creditease.dbus.commons.DbusMessageBuilder;

import org.apache.commons.lang3.StringUtils;

/**
 * Created by Administrator on 2018/5/30.
 */
public class TestUms2DbusMessage {

    public static void main(String[] args) {

        Map<Long, Set<String>> fixOutTableColumnsMap = new HashMap<>();
        Set<String> columns = new HashSet<>();
        columns.add("id");
        columns.add("activitySerial");
        columns.add("customerType");
        columns.add("customerCode");
        fixOutTableColumnsMap.put(1L, columns);

        String strUms = "{\"payload\":[{\"tuple\":[\"69230000014284738\",\"2018-05-30 10:34:33.000\",\"i\",\"17863528\",\"2117700\",\"H2M220180522008\",1,\"C2S2017111400085\",\"C2S2017111400085\",null,\"11080825\",null,\"1109100\",0,1,0,1,\"2018-05-30 10:34:33\",null,\"11080825\",\"11051559\",\"254029\",null,null]}],\"protocol\":{\"type\":\"data_increment_data\",\"version\":\"1.3\"},\"schema\":{\"batchId\":4,\"fields\":[{\"encoded\":false,\"name\":\"ums_id_\",\"nullable\":false,\"type\":\"long\"},{\"encoded\":false,\"name\":\"ums_ts_\",\"nullable\":false,\"type\":\"datetime\"},{\"encoded\":false,\"name\":\"ums_op_\",\"nullable\":false,\"type\":\"string\"},{\"encoded\":false,\"name\":\"ums_uid_\",\"nullable\":false,\"type\":\"string\"},{\"encoded\":false,\"name\":\"id\",\"nullable\":true,\"type\":\"long\"},{\"encoded\":false,\"name\":\"activitySerial\",\"nullable\":true,\"type\":\"string\"},{\"encoded\":false,\"name\":\"customerType\",\"nullable\":true,\"type\":\"int\"},{\"encoded\":false,\"name\":\"customerCode\",\"nullable\":true,\"type\":\"string\"},{\"encoded\":false,\"name\":\"customerId\",\"nullable\":true,\"type\":\"string\"},{\"encoded\":false,\"name\":\"cca\",\"nullable\":true,\"type\":\"string\"},{\"encoded\":false,\"name\":\"managerId\",\"nullable\":true,\"type\":\"string\"},{\"encoded\":false,\"name\":\"partnerId\",\"nullable\":true,\"type\":\"string\"},{\"encoded\":false,\"name\":\"inviteCode\",\"nullable\":true,\"type\":\"string\"},{\"encoded\":false,\"name\":\"smsFlag\",\"nullable\":true,\"type\":\"int\"},{\"encoded\":false,\"name\":\"isApply\",\"nullable\":true,\"type\":\"int\"},{\"encoded\":false,\"name\":\"signUpState\",\"nullable\":true,\"type\":\"int\"},{\"encoded\":false,\"name\":\"state\",\"nullable\":true,\"type\":\"int\"},{\"encoded\":false,\"name\":\"createtime\",\"nullable\":true,\"type\":\"datetime\"},{\"encoded\":false,\"name\":\"modifytime\",\"nullable\":true,\"type\":\"datetime\"},{\"encoded\":false,\"name\":\"operator\",\"nullable\":true,\"type\":\"string\"},{\"encoded\":false,\"name\":\"creator\",\"nullable\":true,\"type\":\"string\"},{\"encoded\":false,\"name\":\"phoneNumber\",\"nullable\":true,\"type\":\"string\"},{\"encoded\":false,\"name\":\"signSource\",\"nullable\":true,\"type\":\"int\"},{\"encoded\":false,\"name\":\"position\",\"nullable\":true,\"type\":\"string\"}],\"namespace\":\"mysql.caiwudb.fso_yao_db.customer_offline.4.0.0\"}}";
        JSONObject ums = JSON.parseObject(strUms);
        JSONObject protocol = ums.getJSONObject("protocol");
        JSONObject schema = ums.getJSONObject("schema");
        DbusMessageBuilder builder = new DbusMessageBuilder(protocol.getString("version"));
        builder.build(convertStr2ProtocolType(protocol.getString("type")), schema.getString("namespace"), schema.getIntValue("batchId"));

        List<Integer> removeIndex = new ArrayList<>();
        List<Integer> reservedIndex = new ArrayList<>();
        JSONArray fields = schema.getJSONArray("fields");
        for (int i=0; i<fields.size(); i++) {
            JSONObject field = fields.getJSONObject(i);
            if (StringUtils.equalsIgnoreCase(field.getString("name"), DbusMessage.Field._UMS_ID_) ||
                StringUtils.equalsIgnoreCase(field.getString("name"), DbusMessage.Field._UMS_OP_) ||
                StringUtils.equalsIgnoreCase(field.getString("name"), DbusMessage.Field._UMS_TS_) ||
                StringUtils.equalsIgnoreCase(field.getString("name"), DbusMessage.Field._UMS_UID_)) {
                reservedIndex.add(i);
                continue;
            }
            if (columns.contains(field.getString("name"))) {
                builder.appendSchema(field.getString("name"), convertStr2DataType(field.getString("type")), field.getBoolean("nullable"), field.getBoolean("encoded"));
                reservedIndex.add(i);
            }
            else {
                removeIndex.add(i);
            }

        }

        JSONArray payload = ums.getJSONArray("payload");
        for (int i=0; i<payload.size(); i++) {
            JSONObject tuple = payload.getJSONObject(i);
            JSONArray jsonArrayValue = tuple.getJSONArray("tuple");
            List<Object> values = jsonArrayValue.toJavaList(Object.class);
            if (reservedIndex.size() > removeIndex.size()) {
                List<Object> values_wk = new LinkedList<>(values);
                for (int idx = removeIndex.size() -1; idx>=0; idx--) values_wk.remove(idx);
                builder.appendPayload(values_wk.toArray());
            } else {
                List values_wk = new ArrayList();
                for (int idx : reservedIndex) values_wk.add(values.get(idx));
                builder.appendPayload(values_wk.toArray());
            }
        }
        System.out.println(builder.getMessage().toString());
    }

    private static DbusMessage.ProtocolType convertStr2ProtocolType(String strProtocol) {
        if (StringUtils.equalsIgnoreCase(strProtocol, DbusMessage.ProtocolType.DATA_INCREMENT_DATA.toString())) {
            return DbusMessage.ProtocolType.DATA_INCREMENT_DATA;
        } else if (StringUtils.equalsIgnoreCase(strProtocol, DbusMessage.ProtocolType.DATA_INCREMENT_HEARTBEAT.toString())) {
            return DbusMessage.ProtocolType.DATA_INCREMENT_HEARTBEAT;
        } else if (StringUtils.equalsIgnoreCase(strProtocol, DbusMessage.ProtocolType.DATA_INCREMENT_TERMINATION.toString())) {
            return DbusMessage.ProtocolType.DATA_INCREMENT_TERMINATION;
        }
        return DbusMessage.ProtocolType.DATA_INITIAL_DATA;
    }

    // STRING, INT, LONG, FLOAT, DOUBLE, BOOLEAN, DATE, DATETIME, DECIMAL, BINARY, RAW;
    public static DataType convertStr2DataType(String type) {
        type = type.toUpperCase();
        DataType datatype = null;
        switch (type) {
            case "INT":
                datatype = DataType.INT;
                break;
            case "LONG":
                datatype = DataType.LONG;
                break;
            case "FLOAT":
                datatype = DataType.FLOAT;
                break;
            case "DOUBLE":
                datatype = DataType.DOUBLE;
                break;
            case "BOOLEAN":
                datatype = DataType.BOOLEAN;
                break;
            case "DATE":
                datatype = DataType.DATE;
                break;
            case "DATETIME":
                datatype = DataType.DATETIME;
                break;
            case "DECIMAL":
                datatype = DataType.DECIMAL;
                break;
            case "BINARY":
                datatype = DataType.BINARY;
                break;
            case "RAW":
                datatype = DataType.RAW;
                break;
            default:
                datatype = DataType.STRING;
                break;
        }
        return datatype;
    }

}
