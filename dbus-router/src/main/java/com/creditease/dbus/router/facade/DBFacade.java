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


package com.creditease.dbus.router.facade;

import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.encoders.EncodePlugin;
import com.creditease.dbus.router.bean.FixColumnOutPutMeta;
import com.creditease.dbus.router.bean.ReadSpoutConfig;
import com.creditease.dbus.router.bean.Resources;
import com.creditease.dbus.router.bean.Sink;
import com.creditease.dbus.router.cache.Cache;
import com.creditease.dbus.router.container.DataSourceContainer;
import com.creditease.dbus.router.dao.IDBusRouterDao;
import com.creditease.dbus.router.dao.impl.DBusRouterDao;
import com.creditease.dbus.router.encode.DBusRouterEncodeColumn;
import com.creditease.dbus.router.util.DBusRouterConstants;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by mal on 2018/5/22.
 */
public class DBFacade {

    private static Logger logger = LoggerFactory.getLogger(DBFacade.class);

    private IDBusRouterDao routerDao = null;

    public DBFacade(Properties props) {
        DataSourceContainer.getInstance().register(DBusRouterConstants.DBUS_ROUTER_DB_KEY, props);
        routerDao = new DBusRouterDao();
    }

    public List<Resources> loadResources(String topologyName, Integer projectTopologyTableId) throws Exception {
        return routerDao.queryResources(topologyName, projectTopologyTableId);
    }

    public ReadSpoutConfig loadReadSpoutConfig(String topologyName) throws Exception {
        ReadSpoutConfig config = new ReadSpoutConfig();
        List<Resources> resources = routerDao.queryResources(topologyName);
        if (resources != null && resources.size() > 0) {
            for (Resources vo : resources) {
                String topic = vo.getTopicName();
                config.getTopics().add(topic);
                String namespace = StringUtils.joinWith(".", vo.getDsName(), vo.getSchemaName(), vo.getTableName());
                config.getNamespaces().add(namespace);
                config.getNamespaceTableIdPair().put(namespace, vo.getTableId());
            }
        }
        logger.info("read spout config: {}", JSONObject.toJSONString(config));
        return config;
    }

    public void loadAliasMapping(String topologyName, Cache cache) throws Exception {
        List<Resources> resources = routerDao.queryResources(topologyName);
        if (resources != null && resources.size() > 0) {
            for (Resources vo : resources) {
                if (StringUtils.isNoneBlank(vo.getAlias())) {
                    cache.putObject(vo.getAlias(), vo.getOriginDsName());
                }
            }
        }
        logger.info("alias mapping: {}", JSONObject.toJSONString(cache));
    }

    public List<Sink> loadSinks(String topologyName) throws Exception {
        return routerDao.querySinks(topologyName);
    }

    public List<Sink> loadSinks(String topologyName, Integer projectTopologyTableId) throws Exception {
        return routerDao.querySinks(topologyName, projectTopologyTableId);
    }

    public List<EncodePlugin> loadEncodePlugins(String topologyName) throws Exception {
        return routerDao.queryEncodePlugins(topologyName);
    }

    public Map<Long, List<DBusRouterEncodeColumn>> loadEncodeConfig(String topologyName) throws Exception {
        return routerDao.queryEncodeConfig(topologyName);
    }

    public Map<Long, List<DBusRouterEncodeColumn>> loadEncodeConfig(String topologyName, Integer projectTopologyTableId) throws Exception {
        return routerDao.queryEncodeConfig(topologyName, projectTopologyTableId);
    }

    public Map<Long, List<FixColumnOutPutMeta>> loadFixColumnOutPutMeta(String topologyName) throws Exception {
        return routerDao.queryFixColumnOutPutMeta(topologyName);
    }

    public Map<Long, List<FixColumnOutPutMeta>> loadFixColumnOutPutMeta(String topologyName, Integer projectTopologyTableId) throws Exception {
        return routerDao.queryFixColumnOutPutMeta(topologyName, projectTopologyTableId);
    }

    public int toggleProjectTopologyTableStatus(Integer id, String status) throws SQLException {
        return routerDao.toggleProjectTopologyTableStatus(id, status);
    }

    public int toggleProjectTopologyStatus(Integer id, String status) throws SQLException {
        return routerDao.toggleProjectTopologyStatus(id, status);
    }

    public int updateProjectTopologyConfig(Integer id, String config) throws SQLException {
        return routerDao.updateProjectTopologyConfig(id, config);
    }

    public boolean isUsingTopic(Integer id) throws SQLException {
        return routerDao.isUsingTopic(id);
    }

    /*public int updateSchemaChangeFlag(Long id, String talbeName, int value) throws SQLException {
        return routerDao.updateSchemaChangeFlag(id, talbeName, value);
    }*/

    public int updateTpttSchemaChange(Long id, int value) throws SQLException {
        return routerDao.updateTpttSchemaChange(id, value);
    }

    public int updateTpttmvSchemaChange(Long id, int value, String changeComment) throws SQLException {
        return routerDao.updateTpttmvSchemaChange(id, value, changeComment);
    }

    public int updateTptteocSchemaChange(Long id, int value, String changeComment) throws SQLException {
        return routerDao.updateTptteocSchemaChange(id, value, changeComment);
    }

}
