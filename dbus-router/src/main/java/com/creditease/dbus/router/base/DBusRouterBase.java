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


package com.creditease.dbus.router.base;

import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.router.container.DataSourceContainer;
import com.creditease.dbus.router.facade.DBFacade;
import com.creditease.dbus.router.facade.ZKFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.Map;
import java.util.Properties;

/**
 * Created by mal on 2018/5/22.
 */
public class DBusRouterBase {

    private static Logger logger = LoggerFactory.getLogger(DBusRouterBase.class);

    /**
     * 链接zk字符串
     */
    public String zkStr = null;

    /**
     * topology id
     */
    public String topologyId = null;

    /**
     * topology alias
     */
    public String alias = null;

    /**
     * project name
     */
    public String projectName = null;

    /**
     * storm topology 配置信息
     */
    public Map conf = null;

    /**
     * router 配置信息
     */
    public Properties routerConfProps;

    /**
     * 操作zk句柄
     */
    public ZKFacade zkHelper = null;

    /**
     * 操作db句柄
     */
    public DBFacade dbHelper = null;

    public DBusRouterBase(Map conf) {
        this.conf = conf;
        zkStr = (String) conf.get(Constants.ZOOKEEPER_SERVERS);
        topologyId = (String) conf.get(Constants.TOPOLOGY_ID);
        projectName = (String) conf.get(Constants.ROUTER_PROJECT_NAME);
        alias = (String) conf.get(Constants.TOPOLOGY_ALIAS);
        logger.info(MessageFormat.format("zk servers:{0}, topology id:{1}, project name:{2}, alias:{3}",
                zkStr, topologyId, projectName, alias));
    }

    public void init() throws Exception {
        zkHelper = new ZKFacade(zkStr, topologyId, projectName);
        dbHelper = new DBFacade(zkHelper.loadMySqlConf());
        routerConfProps = zkHelper.loadRouterConf();
    }

    public void close(boolean isReload) {
        routerConfProps = null;
        zkHelper.close();
        if (!isReload) {
            DataSourceContainer.getInstance().clear();
        }
    }

}
