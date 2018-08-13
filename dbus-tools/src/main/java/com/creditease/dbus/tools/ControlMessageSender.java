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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.creditease.dbus.commons.ControlMessage;
import com.creditease.dbus.commons.ControlType;
import com.creditease.dbus.tools.common.ConfUtils;
import com.google.common.base.Strings;
import org.apache.commons.cli.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.Future;

@Deprecated
public class ControlMessageSender {
    private static Logger logger = LoggerFactory.getLogger(ControlMessageSender.class);

    private final static String CONFIG_FILE = "ControlMessageSender/ControlMessageSender.properties";
    private final static String PRODUCTER_CONFIG = "ControlMessageSender/producer.properties";
    private ControlMessage msg;
    private String server;
    private String topic;
    private String json;
    private String type;
    private Properties producerProps = null;
    private KafkaProducer<String, byte[]> producer = null;

    public ControlMessageSender() throws IOException {
        producerProps = ConfUtils.getProps(PRODUCTER_CONFIG);
    }

    public void send() {
        if (loadConfig() != 0) {
            return;
        }

        producerProps.setProperty("bootstrap.servers", server);  //update
        producer = new KafkaProducer<>(producerProps);

        String key = msg.getType();
        String jsonMessage = msg.toJSONString();

        System.out.println("--------------------- preview your message -------------------------");
        System.out.printf("kafka topic:%s\n", topic);
        System.out.printf("message type:%s\n", type);
        System.out.println("message content:");
        System.out.println(JSON.toJSONString(msg, SerializerFeature.PrettyFormat));
        System.out.println("-------------------------------------------------------------------------");

        System.out.println("send this message(y/n)?");
        Scanner scanner = new Scanner(System.in);

        while (scanner.hasNext()) {
            String cmd = scanner.next().trim();
            if ("N".equalsIgnoreCase(cmd)) {
                System.out.println("give up to send message!");
                return;
            }
            if ("Y".equalsIgnoreCase(cmd)) {
                byte[] message = jsonMessage.getBytes();

                try {
                    Future<RecordMetadata> result = producer.send(new ProducerRecord<>(topic, key, message), null);
                    result.get();
                    //logger.info(String.format("to_topic=%s, key=%s, value=%s", topic, key, jsonMessage));
                    //System.out.println(jsonMessage);
                    System.out.println("message send ok!");

                } catch (Exception err) {
                    err.printStackTrace();
                    System.out.println("message send fail!");
                } finally {
                    producer.close();
                }
                return;
            }
        }
    }

    private String readJsonFile(String filename) throws IOException {
        return new String(ConfUtils.toByteArray(filename));
    }

    private int loadConfig() {

        try {
            Properties props = ConfUtils.getProps(CONFIG_FILE);

            server = props.getProperty("bootstrap.servers");

            if (Strings.isNullOrEmpty(topic)) {
                topic = props.getProperty("topic");
            }

            if (server == null || topic == null) {
                System.out.println("server 和 topic 必须输入!");
                return -1;
            }

            String jsonFile = json; //props.getProperty("json.template");

            if (Strings.isNullOrEmpty(type)) {
                type = props.getProperty("message.type");
            }

            if (jsonFile == null && type == null) {
                System.out.println("json 或 type 必须输入其中一个!");
                return -1;
            }

            if (jsonFile != null) {
                String jsonString = readJsonFile(jsonFile);
                msg = ControlMessage.parse(jsonString);

                Date date = new Date();
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                msg.setTimestamp(sdf.format(date));

            } else {
                msg = new ControlMessage();
                msg.setId(System.currentTimeMillis());
                msg.setType(type);
                msg.setFrom("ControlMessageSender by manual");
            }

            if (ControlType.UNKNOWN == ControlType.getCommand(msg.getType())) {
                throw new RuntimeException("json文件中 type类型错误！" + msg.getType());
            }
            return 0;
        } catch (Exception exp) {
            logger.error("Parsing failed.  Reason: " + exp.getMessage());
            exp.printStackTrace();
            return -2;
        }
    }

    private static int parseCommandArgs(ControlMessageSender sender, String[] args) {
        Options options = new Options();

        options.addOption("json", "json", true, "json file name like conf/appender-reload.json");
        options.addOption("to", "topic", true, "target topic of kafka");
        options.addOption("type", "type", true, "target topic of kafka");

        CommandLineParser parser = new DefaultParser();
        try {
            // parse the command line arguments
            CommandLine line = parser.parse(options, args);

            if (line.hasOption("help")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("dbus-tools.jar", options);

                return -1;
            } else {
                sender.json = line.getOptionValue("json");
                sender.topic = line.getOptionValue("topic");
                sender.type = line.getOptionValue("type");
            }
            return 0;
        } catch (ParseException exp) {
            logger.error("Parsing failed.  Reason: " + exp.getMessage());
            exp.printStackTrace();
            return -2;
        }
    }

    public static void main(String[] args) {
        try {
            ControlMessageSender uploader = new ControlMessageSender();
            parseCommandArgs(uploader, args);
            uploader.send();
        } catch (Exception exp) {
            exp.printStackTrace();
        }
    }
}
