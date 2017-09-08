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

package com.creditease.dbus.tools;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.tools.common.ConfUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Created by 201605240095 on 2016/7/25.
 */
@Deprecated
public class FullPullerPerfChecker {

    private final static String CONSUMER_PROPS = "FullPullerPerfChecker/consumer.properties";
    private final static String ACCEPT_TYPE = "data_increment_data";

    private Consumer<String, String> consumer;
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private HashMap<String, Integer> incrMap = new HashMap<>();
    private HashMap<String, Integer> fullMap = new HashMap<>();

    private static List<String> topics = new LinkedList<>();
    private static int rollBack = 0;
    private static int consumingInterval = 100;
    /**
     * checkFullPullerPerf
     * Check the performance of pulling a whole table
     * @throws Exception
     */
    public void checkFullPullerPerf() throws Exception {
        boolean recievedData = false;
        try {
            this.consumer = createConsumer();
            // Fetch data from the consumer
            while (true) {
                // Wait for 100ms
                Thread.sleep(consumingInterval);
                ConsumerRecords<String, String> records = consumer.poll(0);
                // 按记录进行处理
                for (ConsumerRecord<String, String> record : records) {
                    // Get data of a table
                    JSONObject jo = JSONObject.parseObject(record.value());
                    // Skip all jsons other than pulling data
                    String type = jo.getJSONObject("protocol").getString("type");

                    if (type.equals(ACCEPT_TYPE)) {
                        JSONObject schemaObject = jo.getJSONObject("schema");
                        // Get necessary info to access dbs
                        String namespace = schemaObject.getString("namespace");

                        JSONArray dataArr = jo.getJSONArray("payload");

                        int numOfData = dataArr.size();

                        // 查看是增量还是全量,看要往哪个table里放
                        int id = dataArr.getJSONObject(0).getJSONArray("tuple").getInteger(0);
                        boolean recievedIncrData = (id != 0);
                        putIntoMap(recievedIncrData, namespace, numOfData, schemaObject, dataArr);
                        recievedData = true;
                    }
                }
                
                // After reading a piece of data, we print it out.
                reportCurrentFullPullerData(recievedData);
                recievedData = false;
            }

        } catch (Exception e) {
            logger.error("Exception was caught when checking the performance of the full data puller. ");
            throw e;
        } finally {
            // TODO: Will this line be reached?
            logger.info("Finished checking the performance of full data puller. ");
        }
    }

    /**
     * For printing out the report
     */
    private void reportCurrentFullPullerData(boolean recievedData) {
        if(recievedData){
            System.out.println("----------------------------------------");
            System.out.println("current full puller data received - 全量");
            System.out.println("----------------------------------------");
            System.out.format("%50s %30s %n", "namespace", "rowsReceived");
            for (String namespace: fullMap.keySet()) {
                System.out.format("%50s %30d %n", namespace, fullMap.get(namespace));
            }

            System.out.println("----------------------------------------");
            System.out.println("current full puller data received - 增量");
            System.out.println("----------------------------------------");
            System.out.format("%50s %30s %n", "namespace", "rowsReceived");
            for (String namespace: incrMap.keySet()) {
                System.out.format("%50s %30d %n", namespace, incrMap.get(namespace));
            }    
        }else{
            System.out.println("=============================Waiting for data=============================");
        }
    }

    /**
     * fieldsFixedPartIsCorrect
     * Check the fixed parts _ums_id_, _ums_ts_, _ums_op_ are correct
     * @param fields
     * @return
     */
    private boolean fieldsFixedPartIsCorrect(JSONArray fields) {
        JSONObject jo1 = fields.getJSONObject(0);
        if (!jo1.getString("name").equals("_ums_id_") || !jo1.getString("type").equals("long")
                || jo1.getBoolean("nullable")) {
            return false;
        }
        JSONObject jo2 = fields.getJSONObject(1);
        if (!jo2.getString("name").equals("_ums_ts_") || !jo2.getString("type").equals("datetime")
                || jo2.getBoolean("nullable")) return false;

        JSONObject jo3 = fields.getJSONObject(2);
        if (!jo3.getString("name").equals("_ums_op_") || !jo3.getString("type").equals("string")
                || jo3.getBoolean("nullable")) return false;

        return true;
    }


    private void putIntoMap(boolean incr, String namespace, int numOfData, JSONObject schemaObject, JSONArray dataArr) {

        HashMap<String, Integer> map;

        if (incr) {
            map = incrMap;
        } else map = fullMap;

        // Update count of the row to the hashmap
        if (map.containsKey(namespace)) {
            map.put(namespace, map.get(namespace) + numOfData);
        } else { // Receive a row with new namespace

            JSONArray fields = schemaObject.getJSONArray("fields");

            // If it's the first element that is inserted, need to check
            // if the basic format is correct
            // Check if the fixed field element is correct
            if (!fieldsFixedPartIsCorrect(fields)) {
                logger.error(String.format("Received a record having mismatched fixed fields with namespace %s! %n" +
                                "Fields: %s. %n" +
                                "Please check. Abort.",
                        namespace, fields.toString()));
                return;
            }

            // Check each record has the column number match
            int fieldLen = fields.size();
            for (int i = 0; i < numOfData; i++) {
                int dataColumnSize = dataArr.getJSONObject(i).getJSONArray("tuple").size();
                if (dataColumnSize != fieldLen) {
                    logger.error(String.format("There is a row on payload which has column size that does not match the " +
                                    "correct column size! Received size = %d, should be = %d. namespace = %s. Abort. ",
                            dataColumnSize, fieldLen, namespace));
                    return;
                }
            }

            map.put(namespace, numOfData);
        }
    }

    /**
     * createConsumer - create a new consumer
     * @return
     * @throws Exception
     */
    private Consumer<String, String> createConsumer() throws Exception {
        Properties props = ConfUtils.getProps(CONSUMER_PROPS);
        Consumer<String, String> consumer = new KafkaConsumer<>(props);
       
        // Seek to end automatically
        List<TopicPartition> pts = topics.stream().map(s -> new TopicPartition(s, 0)).collect(Collectors.toList());
        consumer.assign(pts);
        if(rollBack==0){
            consumer.seekToEnd(pts);  
        }else{
            for (TopicPartition topicPartition : pts) {
                consumer.seek(topicPartition, consumer.position(topicPartition)-rollBack);
                logger.info("Consumer seeked to -500000 :"+consumer.position(topicPartition));
            }    
        }  
        return consumer;
    }

    public static void main(String[] args) throws Exception {
        if (args.length <= 2) {
            System.out.println("Please provide sufficient param. ");
            return;
        }
        int paramIndex=0; 
        rollBack = Integer.parseInt(args[paramIndex++]);
        consumingInterval = Integer.parseInt(args[paramIndex++]);
        for (;paramIndex < args.length; paramIndex++) {
            topics.add(args[paramIndex]);
        }
        FullPullerPerfChecker checker = new FullPullerPerfChecker();
        checker.checkFullPullerPerf();
    }
}
