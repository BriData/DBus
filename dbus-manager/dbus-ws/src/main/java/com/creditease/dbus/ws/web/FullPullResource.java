/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2017 Bridata
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

package com.creditease.dbus.ws.web;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.commons.ControlMessage;
import com.creditease.dbus.commons.DbusMessage;
import com.creditease.dbus.mgr.base.ConfUtils;
import com.creditease.dbus.utils.ControlMessageSender;
import com.creditease.dbus.ws.common.Constants;
import com.creditease.dbus.ws.common.Result;
import com.creditease.dbus.ws.tools.ControlMessageSenderProvider;
import com.creditease.dbus.ws.tools.ZookeeperServiceProvider;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import java.util.*;

@Path("/fullPull")
@Consumes("application/json")
@Produces("application/json;charset=utf-8")
public class FullPullResource {
    private Logger logger = LoggerFactory.getLogger(getClass());
    /*
    private static KafkaConsumer<String, byte[]>  consumer;
    static {
        Properties consumerProps = null;
        try {
            consumerProps = PropertiesUtils.getProps("consumer.properties");
        } catch (IOException e) {
            e.printStackTrace();
        }
        consumerProps.setProperty("client.id","full-pull.reader");
        consumerProps.setProperty("group.id","full-pull.reader");
        //consumerProps.setProperty("bootstrap.servers", PropertiesUtils.getProps("application.properties").getProperty("kafka.bootstrap.servers"));

        consumer = new KafkaConsumer(consumerProps);
    }
   */
    @POST
    @Path("send")
    public Response sendMessage(Map<String, String> map) {
        String topic = null;
        String strMessage = null;
        try {
            if (!validate(map)) {
                throw new Exception("参数验证失败");
            }
            strMessage = map.get("message");

            ControlMessage message = ControlMessage.parse(strMessage);
            if(!validate(message)) {
                throw new Exception("消息验证失败");
            }
            topic = map.get("topic");

            Map<String, Object> payload = message.getPayload();
            String key = "";
            String value = "";

            Properties consumerProps = ConfUtils.load("consumer.properties");
            consumerProps.setProperty("client.id","full-pull.reader");
            consumerProps.setProperty("group.id","full-pull.reader");

            ZookeeperServiceProvider zk = ZookeeperServiceProvider.getInstance();
            Properties globalConf = zk.getZkService().getProperties(Constants.GLOBAL_CONF);
            consumerProps.setProperty("bootstrap.servers", globalConf.getProperty("bootstrap.servers"));

            KafkaConsumer<String, byte[]> consumer = new KafkaConsumer(consumerProps);

            String outputTopic = map.get("outputTopic");
            logger.info("[control message] Send control message to ctrlTopic: {} \n outputTopic: {} \n map: {}", topic,outputTopic,map);

            TopicPartition dataTopicPartition = new TopicPartition(outputTopic, 0);
            List<TopicPartition> topics = Arrays.asList(dataTopicPartition);
            consumer.assign(topics);
            //long offset0 = consumer.position(dataTopicPartition);
            consumer.seekToEnd(topics);
            long offset = consumer.position(dataTopicPartition);
            final String OP_TS = "OP_TS";
            long step = Integer.valueOf(consumerProps.getProperty("max.poll.records"));
            if(message.getPayload().get("POS") == null || message.getPayload().get(OP_TS) == null) {
                ConsumerRecords<String, byte[]> results = consumer.poll(100);
                while (results.isEmpty()) {
                    offset = offset - step ;
                    if(offset < 0) {
                        payload.put("POS", 0);
                        payload.put(OP_TS, DateFormatUtils.format(Calendar.getInstance(), "yyyy-MM-dd HH:mm:ss.SSS"));
                        message.setPayload(payload);
                        break;
                    }
                    consumer.seek(dataTopicPartition, offset);
                    results = consumer.poll(100);
                }

                for (ConsumerRecord record : results) {
                    key = record.key().toString();
                    value = record.value().toString();
                    JSONObject jsonDbusMessage = JSON.parseObject(value);
                    int iPos = findDbusMessageFieldIndex(jsonDbusMessage, DbusMessage.Field._UMS_ID_);
                    int iOpTs = findDbusMessageFieldIndex(jsonDbusMessage, DbusMessage.Field._UMS_TS_);
                    if(iPos == -1 || iOpTs == -1) continue;
                    String pos = jsonDbusMessage.getJSONArray("payload").getJSONObject(0).getJSONArray("tuple").getString(iPos);
                    String op_ts = jsonDbusMessage.getJSONArray("payload").getJSONObject(0).getJSONArray("tuple").getString(iOpTs);
                    if (key.indexOf("data_increment_heartbeat") != -1 || key.indexOf("data_increment_data") != -1) {
                        logger.info("pos : {} ,op_ts : {}", pos, op_ts);
                        payload.put("POS", pos);
                        payload.put(OP_TS, op_ts);
                        message.setPayload(payload);
                        break;
                    }
                }
            }

            ControlMessageSender sender = ControlMessageSenderProvider.getInstance().getSender();
            sender.send(topic, message);
            logger.info("[control message] Send control message to topic[{}] success.\n message.pos: {} \n message.op_ts: {}", topic, message.getPayload().get("POS") , message.getPayload().get(OP_TS));
            return Response.ok().entity(Result.OK).build();
        } catch (Exception e) {
            logger.error("[control message] Error encountered while sending control message.\ncontrol topic:{}\nmessage:{}", topic,strMessage, e);
            return Response.status(200).entity(new Result(-1, e.getMessage())).build();
        }
    }

    private int findDbusMessageFieldIndex(JSONObject jsonDbusMessage, String string) {
        try {
            JSONObject jsonSchema = jsonDbusMessage.getJSONObject("schema");
            JSONArray jsonFields = jsonSchema.getJSONArray("fields");
            for(int i=0;i<jsonFields.size();i++) {
                if(jsonFields.getJSONObject(i).getString("name").equals(string)) return i;
            }
        } catch (Exception e) {
            logger.error("[control message] Parsing dbusmessage to json failed, message: {}", e);
        }
        return -1;
    }

    private boolean validate(Map<String, String> map) {
        if (map == null) return false;
        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (StringUtils.isBlank(entry.getValue())) {
                return false;
            }
        }
        return true;
    }
    private boolean validate(ControlMessage message) {
        if(message.getId() <= 0) return false;
        if(StringUtils.isBlank(message.getType())) return false;
        if(StringUtils.isBlank(message.getFrom())) return false;
        if(StringUtils.isBlank(message.getTimestamp())) return false;
        return true;
    }
}
