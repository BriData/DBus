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


package com.creditease.dbus.stream.mongo.appender.convertor;

import avro.shaded.com.google.common.collect.Maps;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.commons.ControlMessage;
import com.creditease.dbus.commons.DBusConsumerRecord;
import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.appender.utils.Pair;
import com.creditease.dbus.stream.common.appender.utils.PairWrapper;
import com.creditease.dbus.stream.common.appender.utils.Utils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * Created by ximeiwang on 2017/12/21.
 */
public class Convertor {
    private static final String FULL_DATA_PULL_REQ = "FULL_DATA_PULL_REQ";

    //public static ControlMessage mongoFullPullMessage(String entry, String topologyId, ConsumerRecord<String, byte[]> consumerRecord){
    public static ControlMessage mongoFullPullMessage(String entry, String topologyId, DBusConsumerRecord<String, byte[]> consumerRecord) {
        JSONObject entryJson = JSON.parseObject(entry);

        ControlMessage message = new ControlMessage();
        message.setId(System.currentTimeMillis());
        message.setFrom(topologyId);
        message.setType(FULL_DATA_PULL_REQ);
        message.addPayload("topic", consumerRecord.topic());
        message.addPayload("DBUS_DATASOURCE_ID", Utils.getDatasource().getId());

        long btime = entryJson.getLong("_ts");
        long timepart = btime >> 32;
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String time_ts = format.format(new Date(timepart * 1000L));

        message.addPayload("POS", entryJson.getString("_ts"));
        message.addPayload("OP_TS", time_ts + ".000");

        JSONObject document = JSON.parseObject(entryJson.getString("_o"));
        for (String key : document.keySet()) {
            message.addPayload(key.toUpperCase(), document.getString(key));
        }

        return message;
    }

    public static <T extends Object> PairWrapper<String, Object> convertProtobufRecord(String entryMessage, String document) {
        PairWrapper<String, Object> wrapper = new PairWrapper<>();
        JSONObject entryJson = JSON.parseObject(entryMessage);

        long btime = entryJson.getLong("_ts");
        long timepart = btime >> 32;
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String time_ts = format.format(new Date(timepart * 1000L));

        wrapper.addProperties(Constants.MessageBodyKey.POS, entryJson.getString("_ts"));
        wrapper.addProperties(Constants.MessageBodyKey.OP_TS, time_ts + ".000");

        Map<String, Object> map = convert2map(document);

        for (Map.Entry<String, Object> entry : map.entrySet()) {
            //wrapper.addPair(new Pair<>(entry.getKey(), CharSequence.class.isInstance(entry.getValue())?entry.getValue().toString():entry.getValue()));
            wrapper.addPair(new Pair<>(entry.getKey(), entry.getValue()));
        }

        return wrapper;
    }

    private static Map<String, Object> convert2map(String document) {
        Map<String, Object> map = Maps.newHashMap();
        if (document != null && !document.isEmpty()) {
            JSONObject documentJson = JSON.parseObject(document);
            for (String key : documentJson.keySet()) {
                map.put(key, documentJson.getString(key));
            }
        }
        return map;
    }


}
