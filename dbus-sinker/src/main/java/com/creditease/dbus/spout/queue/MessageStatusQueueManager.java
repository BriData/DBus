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


package com.creditease.dbus.spout.queue;

import com.creditease.dbus.commons.DBusConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * 原则：at least once
 * 1、只要ok过，就不再修改其状态；
 * 2、遇到ok,就检查其前面的状态是否全ok，并将全ok的提交
 * 3、遇到fail，检查其前面是否还有fail,并找到状态为fail的offset最小的seek
 * |                     | element not existed   | State = Init | State = OK     | State = FAIL          |
 * | addMessage          | added in, set as Init |  Skip it     |  Skip it       | Init                  |
 * | okAndGetCommitPoint | Impossible            |  OK          |  OK            | OK                    |
 * | failAndGetSeekPoint | Impossible            | FAIL         |  Skip it       | FAIL                  |
 */
public class MessageStatusQueueManager {
    private Logger logger = LoggerFactory.getLogger(getClass());
    private Map<String, MessageStatusQueue> queueMap;

    public MessageStatusQueueManager() {
        queueMap = new HashMap<>();
    }

    public void addMessage(DBusConsumerRecord<String, byte[]> record) {
        getQueue(buildKey(record)).add(record);
    }

    public DBusConsumerRecord<String, byte[]> okAndGetCommitPoint(DBusConsumerRecord<String, byte[]> record) {
        MessageStatusQueue queue = getQueue(buildKey(record));
        QueueElement element = queue.getQueueElement(record.offset());
        if (element == null) {
            logger.warn("okAndGetCommitPoint impossible!!! message[topic:{},partition:{},offset:{}] not found in the queue.", record.topic(), record.partition(), record.offset());
            return null;

        }
        element.ok();
        logger.debug("message[topic:{},partition:{},offset:{},key:{}] status was set to {}.", record.topic(), record.partition(), record.offset(), record.key(), "'ok'");

        QueueElement e = queue.commitPoint();
        if (e == null) {
            logger.debug("commit point was not found.");
            return null;
        }
        return e.getRecord();
    }

    public void committed(DBusConsumerRecord<String, byte[]> record) {
        getQueue(buildKey(record)).popOKElements();
    }

    public DBusConsumerRecord<String, byte[]> failAndGetSeekPoint(DBusConsumerRecord<String, byte[]> record) {
        MessageStatusQueue queue = getQueue(buildKey(record));
        QueueElement element = queue.getQueueElement(record.offset());
        if (element == null) {
            logger.warn("failAndGetSeekPoint impossible!!! message[topic:{},partition:{},offset:{}] not found in the queue.", record.topic(), record.partition(), record.offset());
            return null;
        }

        element.fail();

        QueueElement e = queue.seekPoint();
        if (e == null) {
            logger.warn("seek point was not found.");
            return null;
        }
        return e.getRecord();
    }

    public boolean isAllMessageProcessed() {
        for (Map.Entry<String, MessageStatusQueue> entry : queueMap.entrySet()) {
            if (!entry.getValue().isEmpty()) {
                logger.info("[isAllMessageProcessed] queue {}, size:{} .", entry.getKey(), entry.getValue().size());
                return false;
            }
        }
        return true;
    }

    private String buildKey(DBusConsumerRecord<String, byte[]> record) {
        return record.topic() + "-" + record.partition();
    }

    private MessageStatusQueue getQueue(String key) {
        MessageStatusQueue queue = queueMap.get(key);
        if (queue == null) {
            queue = new MessageStatusQueue();
            queueMap.put(key, queue);
        }
        return queue;
    }

    public DBusConsumerRecord<String, byte[]> getCommitPoint(String key) {
        if (queueMap.get(key) == null) {
            return null;
        }
        QueueElement e = queueMap.get(key).commitPoint();
        if (e == null) {
            logger.debug("commit point was not found.");
            return null;
        }
        return e.getRecord();
    }

    public void removeAll(){
        queueMap.clear();
        logger.warn("received ctrl message, remove all queue message");
    }

}
