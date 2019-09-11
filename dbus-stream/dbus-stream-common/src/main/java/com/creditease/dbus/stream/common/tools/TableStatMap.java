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

package com.creditease.dbus.stream.common.tools;

import com.creditease.dbus.commons.StatMessage;
import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.appender.bean.DataTable;
import com.creditease.dbus.stream.common.appender.bean.TabSchema;
import com.creditease.dbus.stream.common.appender.cache.ThreadLocalCache;
import com.creditease.dbus.stream.common.appender.utils.DBFacade;
import com.creditease.dbus.stream.common.appender.utils.DBFacadeManager;
import com.creditease.dbus.stream.common.appender.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

/**
 * key = schemaName.tableName
 */
public class TableStatMap extends HashMap<String, StatMessage> {
    protected Logger logger = LoggerFactory.getLogger(getClass());
    private String dsName;

    public void initDsName(String dsName) {
        this.dsName = dsName;
    }

    private String makeKey(String schemaName, String tableName) {
        return String.format("%s.%s", schemaName, tableName).toLowerCase();
    }

    private String getTable(String key) {
        return key.substring(key.indexOf(".") + 1, key.length());
    }

    public void mark(String schemaName, String tableName, long count) {
        String key = makeKey(schemaName, tableName);
        StatMessage message = this.get(key);
        if (message == null) {
            //处理table regex
            //同时这样处理不需要先有checkpoint,有数据即可加入,一次checkpoint即将数据shuffle
            try {
                //这里没有使用ThreadLocal.get CacheNames.DATA_TABLES, 保证mark的表一定可以加入到statMap,这对统计数据量是有好处的，否则可能漏掉一部分统计量
                DBFacade db = DBFacadeManager.getDbFacade();
                TabSchema s = db.queryDataSchema(Utils.getDatasource().getId(), schemaName);
                List<DataTable> tables = db.queryDataTables(s.getId());
                DataTable dt = tables.stream().filter(t -> tableName.matches(t.getPhysicalTableRegex()))
                        .findFirst().orElse(null);
                if (dt != null) {
                    message = new StatMessage(dsName, schemaName, tableName, StatMessage.DISPATCH_TYPE);
                    this.put(key, message);
                    logger.info("[dispatcher] add new StatMessage for {}", key);
                } else {
                    logger.info("mark key:{}", key);
                    return;
                }
            } catch (Exception e) {
                logger.error("[dispatcher] Query data table error {}", e);
                return;
            }
        }
        //count++
        message.addCount(count);
    }

    public List<StatMessage> logMeter(String schemaName, String tableName, long checkpointMS, long txTimeMS) {
        //对模板表进行输出的时候,处理正则表。正则表是没有checkpoint的，需要依靠模板表
        String key = makeKey(schemaName, tableName);
        DataTable t = ThreadLocalCache.get(Constants.CacheNames.DATA_TABLES, key);
        List<StatMessage> msgs = null;
        if (t != null) {
            msgs = entrySet().stream().filter(e -> getTable(e.getKey()).matches(t.getPhysicalTableRegex()))
                    .map(Entry::getValue).collect(Collectors.toList());
        }
        if (msgs == null || msgs.isEmpty()) {
            StatMessage message = new StatMessage(dsName, schemaName, tableName, StatMessage.DISPATCH_TYPE);
            logger.info("logMeter put key:{}", key);
            this.put(key, message);
            msgs = Collections.singletonList(message);
        }
        msgs.forEach(m -> {
            m.setCheckpointMS(checkpointMS);
            m.setTxTimeMS(txTimeMS);
            m.setLocalMS(System.currentTimeMillis());
        });

        return msgs;
    }
}
