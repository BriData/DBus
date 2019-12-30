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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ShardsProcessManager {
    private Logger logger = LoggerFactory.getLogger(getClass());
    private Map<Long, ShardElementQueue> queueMap;

    public ShardsProcessManager() {
        queueMap = new HashMap<>();
    }

    public void addMessage(long key, long offset, long index) {
        getQueue(key).add(offset, index);
    }

    public ShardElement okAndGetCommitPoint(long key, long offset) {
        ShardElementQueue queue = getQueue(key);
        ShardElement element = queue.getShardElement(offset);
        if (element == null) {
            logger.warn("okAndGetCommitPoint impossible!!! message[key:{},offset:{}] not found in the queue.", key, offset);
            return null;

        }
        element.ok();
        logger.debug("message[key:{},offset:{}] status was set to {}.", key, offset, "'ok'");

        ShardElement e = queue.commitPoint();
        if (e == null) {
            logger.debug("commit point was not found.");
            return null;
        }
        return e;
    }

    public void committed(long key) {
        getQueue(key).popOKElements();
    }

    public void failAndClearShardElementQueue(long key, long offset) {
        queueMap.remove(key);
        logger.info("failAndClearShardElementQueue complete!!! message[key:{},offset:{}].", key, offset);

    }

    public boolean isAllMessageProcessed(long key) {
        ShardElementQueue queue = getQueue(key);
        logger.info("queue size:{} while an event arrived.", queue.size());
        return queue.isEmpty();
    }

    public boolean isAllMessageProcessed() {
        for (Map.Entry<Long, ShardElementQueue> entry : queueMap.entrySet()) {
            if (!entry.getValue().isEmpty()) {
                logger.info("[isAllMessageProcessed] queue {}, size:{} .", entry.getKey(), entry.getValue().size());
                return false;
            }
        }
        return true;
    }

    private ShardElementQueue getQueue(long key) {
        ShardElementQueue queue = queueMap.get(key);
        if (queue == null) {
            queue = new ShardElementQueue();
            queueMap.put(key, queue);
        }
        return queue;
    }

    public List<ShardElement> getCommitPoints() {
        List<ShardElement> commitPoints = new ArrayList<>();
        queueMap.entrySet().forEach(entry -> {
            ShardElement queueElement = entry.getValue().commitPoint();
            if (queueElement != null) {
                commitPoints.add(queueElement);
            }
        });
        return commitPoints;
    }

    public ShardElement getCommitPoint(String key) {
        if (queueMap.get(key) == null) {
            return null;
        }
        ShardElement e = queueMap.get(key).commitPoint();
        if (e == null) {
            logger.debug("commit point was not found.");
            return null;
        }
        return e;
    }

}
