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


package com.creditease.dbus.log.processor.base;

import com.creditease.dbus.log.processor.container.DataSourceContainer;
import com.creditease.dbus.log.processor.helper.DbHelper;
import com.creditease.dbus.log.processor.helper.ZkHelper;
import com.creditease.dbus.log.processor.util.Constants;
import com.creditease.dbus.log.processor.vo.DBusDataSource;
import com.creditease.dbus.log.processor.vo.RuleInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by Administrator on 2017/10/12.
 */
public class LogProcessorBase {

    private static Logger logger = LoggerFactory.getLogger(LogProcessorBase.class);

    public DBusDataSource dbusDsConf = null;
    public Map<Long, Map<Long, List<RuleInfo>>> rules = null;
    public Map<String, String> activeTableToTopicMap = null;
    public Map<String, String> abortTableToTopicMap = null;
    public ZkHelper zkHelper = null;
    public String zkStr = null;
    public String topologyId = null;
    public Map conf = null;
    public Properties logProcessorConf = null;

    public LogProcessorBase(Map conf) {
        this.conf = conf;
        zkStr = (String) conf.get(com.creditease.dbus.commons.Constants.ZOOKEEPER_SERVERS);
        logger.info("zk servers: {}", zkStr);
        topologyId = (String) conf.get(Constants.TOPOLOGY_ID);
        logger.info("topologyId: {}", topologyId);
        zkHelper = new ZkHelper(zkStr, topologyId);
    }

    public boolean loadConf() {
        boolean isOk = true;
        try {
            logProcessorConf = zkHelper.loadLogProcessorConf();
            // 创建数据库连接池
            DataSourceContainer.getInstance().register(Constants.DBUS_CONFIG_DB_KEY, zkHelper.loadMySqlConf());
            // 获取log processor的数据库配置信息
            dbusDsConf = DbHelper.loadDBusDataSourceConf(logProcessorConf.getProperty(Constants.LOG_DS_NAME));
            // 获取log processor规则配置信息
            activeTableToTopicMap = new HashMap<>();
            rules = DbHelper.loadRuleInfoConf(logProcessorConf.getProperty(Constants.LOG_DS_NAME), activeTableToTopicMap);
            abortTableToTopicMap = new HashMap<>();
            DbHelper.loadAbortTable(logProcessorConf.getProperty(Constants.LOG_DS_NAME), abortTableToTopicMap);
        } catch (Exception e) {
            logger.error("load conf error: {}", e);
            isOk = false;
        }
        return isOk;
    }

    public void close(boolean isReload) {
        abortTableToTopicMap.clear();
        abortTableToTopicMap = null;
        activeTableToTopicMap.clear();
        activeTableToTopicMap = null;
        rules.clear();
        rules = null;
        dbusDsConf = null;
        if (!isReload) {
            zkHelper.close();
            DataSourceContainer.getInstance().clear();
        }
    }
}
