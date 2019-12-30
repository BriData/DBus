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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class MessageStatusQueue {
    private List<QueueElement> queue;

    public MessageStatusQueue() {
        queue = new ArrayList<>();
    }

    /**
     * 添加kafka message到队列，
     * 如果队列中存在该消息并且状态没有被标记为失败则不会重复添加
     *
     * @param record
     * @return
     */
    public void add(DBusConsumerRecord<String, byte[]> record) {
        QueueElement elem = getQueueElement(record.offset());
        if (elem != null) {
            if (elem.isFailed()) {
                elem.setStatus(QueueElement.INIT);
            }
            elem.setRecord(record);
        } else {
            queue.add(new QueueElement(record));
        }
    }

    public QueueElement seekPoint() {
        for (QueueElement e : queue) {
            if (e.isFailed()) return e;
        }
        return null;
    }

    public QueueElement commitPoint() {
        QueueElement element = null;
        for (QueueElement e : queue) {
            if (e.isOk() && e.getEmitCount() == 0) {
                element = e;
            } else {
                break;
            }
        }
        return element;
    }

    public void popOKElements() {
        for (Iterator<QueueElement> it = queue.iterator(); it.hasNext(); ) {
            QueueElement e = it.next();
            if (e.isOk() && e.getEmitCount() == 0) {
                it.remove();
            } else {
                break;
            }
        }
    }

    public boolean isEmpty() {
        return queue.isEmpty();
    }

    public int size() {
        return queue.size();
    }

    public QueueElement getQueueElement(long offset) {
        for (QueueElement element : queue) {
            if (offset == element.getKey()) return element;
        }
        return null;
    }

}
