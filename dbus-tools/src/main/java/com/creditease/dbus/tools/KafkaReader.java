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


package com.creditease.dbus.tools;

import com.creditease.dbus.tools.common.AbstractSignalHandler;
import com.creditease.dbus.tools.common.ConfUtils;
import org.apache.commons.cli.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Created by 201605240095 on 2016/7/25.
 */
public class KafkaReader extends AbstractSignalHandler {
    private final String CONSUMER_PROPS = "KafkaReader/consumer.properties";
    private Logger logger = LoggerFactory.getLogger(KafkaReader.class);

    private String topicName = "";
    private long offset = -1;
    private long maxLength = 1;
    private Consumer<String, String> consumer;

    /**
     * run
     * Check the performance of pulling a whole table
     *
     * @throws Exception
     */
    public void run() throws Exception {
        int readCount = 0;
        try {
            this.consumer = createConsumer();
            // Fetch data from the consumer
            while (running) {
                // Wait for 100ms
                ConsumerRecords<String, String> records = consumer.poll(1000);
                if (records.count() == 0) {
                    System.out.print(".");
                    continue;
                }
                for (ConsumerRecord<String, String> record : records) {
                    if (readCount >= maxLength) {
                        running = false;
                        break;
                    }
                    readCount++;

                    System.out.println("");
                    System.out.println("offset: " + record.offset() + ", key:" + record.key());
                    System.out.println(record.value());
                }
            }

        } catch (Exception e) {
            logger.error("Exception was caught when read kafka", e);
            throw e;
        } finally {
            System.out.println("");

            consumer.close();
            logger.info("Finished read kafka");
        }
    }

    /**
     * createConsumer - create a new consumer
     *
     * @return
     * @throws Exception
     */
    private Consumer<String, String> createConsumer() throws Exception {

        // Seek to end automatically
        TopicPartition dataTopicPartition = new TopicPartition(topicName, 0);
        List<TopicPartition> topics = Arrays.asList(dataTopicPartition);

        Properties props = ConfUtils.getProps(CONSUMER_PROPS);
        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.assign(topics);

        if (offset == -1) {
            consumer.seekToEnd(topics);
            logger.info("Consumer seek to end");
        } else {
            consumer.seek(dataTopicPartition, offset);
            logger.info(String.format("read changed as offset: %s", consumer.position(dataTopicPartition)));
        }
        return consumer;
    }

    private int parseCommandArgs(String[] args) {
        Options options = new Options();

        options.addOption("t", "topic", true, "the topic you want to read");
        options.addOption("o", "offset", true, "the offset you want to read");
        options.addOption("m", "maxlength", true, "target topic of kafka");

        CommandLineParser parser = new DefaultParser();
        try {
            // parse the command line arguments
            CommandLine line = parser.parse(options, args);

            if (line.hasOption("help")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("dbus-tools.jar com.creditease.dbus.tools.ControlMessageSender", options);

                return -1;
            } else {
                if (!line.hasOption("topic")) {
                    HelpFormatter formatter = new HelpFormatter();
                    formatter.printHelp("dbus-tools.jar com.creditease.dbus.tools.ControlMessageSender ", options);
                    return -1;
                }

                this.topicName = line.getOptionValue("topic");
                logger.info("read topic : {}", this.topicName);
                if (line.hasOption("offset")) {
                    this.offset = Long.parseLong(line.getOptionValue("offset"));
                    logger.info("read offset : {}", this.offset);

                }

                if (line.hasOption("maxlength")) {
                    this.maxLength = Long.parseLong(line.getOptionValue("maxlength"));
                    logger.info("read maxlength : {}", this.maxLength);
                }
            }
            return 0;
        } catch (ParseException exp) {
            logger.error("Parsing failed.  Reason: " + exp.getMessage());
            exp.printStackTrace();
            return -2;
        }
    }

    public static void main(String[] args) throws Exception {
        KafkaReader reader = new KafkaReader();

        if (reader.parseCommandArgs(args) != 0) {
            return;
        }
        reader.run();
    }


}
