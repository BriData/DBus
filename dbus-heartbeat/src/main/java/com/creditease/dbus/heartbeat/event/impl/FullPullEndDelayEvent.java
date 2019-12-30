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


package com.creditease.dbus.heartbeat.event.impl;

import com.creditease.dbus.heartbeat.container.EventContainer;
import com.creditease.dbus.heartbeat.event.AbstractEvent;
import com.creditease.dbus.heartbeat.type.DelayItem;
import org.apache.commons.lang.StringUtils;

import java.util.Iterator;
import java.util.Set;

public class FullPullEndDelayEvent extends AbstractEvent {

    public FullPullEndDelayEvent(long interval) {
        super(interval);
    }

    @Override
    public void run() {
        while (isRun.get()) {
            try {
                DelayItem item = EventContainer.getInstances().takeDealyItem();
                if (!EventContainer.getInstances().containsFullPullerSchema(item.getSchema())) {
                    Iterator<DelayItem> it = EventContainer.getInstances().dqIt();
                    boolean isCanRemove = true;
                    while (it.hasNext()) {
                        DelayItem di = it.next();
                        if (StringUtils.equals(item.getSchema(), di.getSchema())) {
                            isCanRemove = false;
                            break;
                        }
                    }
                    if (isCanRemove && !EventContainer.getInstances().containsFullPullerSchema(item.getSchema())) {
                        Set<String> schemas = EventContainer.getInstances().getSchemasOfTargetTopic(item.getSchema());
                        if (schemas != null) {
                            try {
                                EventContainer.getInstances().fullPullerLock();
                                for (String schema : schemas) {
                                    EventContainer.getInstances().removeSkipSchema(schema);
                                }
                                LOG.info("[fullPullEndDelay-event] 从全量拉取延时队列中删除schema:{}", item.getSchema());
                            } catch (Exception e) {
                                LOG.error("[fullPullEndDelay-event] ", e);
                            } finally {
                                EventContainer.getInstances().fullPullerUnLock();
                            }
                        }
                    } else {
                        LOG.warn("[fullPullEndDelay-event] 延时队列中还有相同的schema:{},不能从全量拉取延时队列中删除.", item.getSchema());
                    }
                }
            } catch (InterruptedException e) {
                LOG.warn("[fullPullEndDelay-event] 全量拉取结束延时线程被中断.");
            } catch (Exception e) {
                LOG.error("[fullPullEndDelay-event]", e);
            }
        }
    }

}
