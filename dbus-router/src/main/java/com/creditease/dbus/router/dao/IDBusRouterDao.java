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


package com.creditease.dbus.router.dao;

import com.creditease.dbus.encoders.EncodePlugin;
import com.creditease.dbus.router.bean.FixColumnOutPutMeta;
import com.creditease.dbus.router.bean.Resources;
import com.creditease.dbus.router.bean.Sink;
import com.creditease.dbus.router.encode.DBusRouterEncodeColumn;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * Created by mal on 2018/5/22.
 */
public interface IDBusRouterDao {

    List<Resources> queryResources(String topoName) throws SQLException;

    List<Resources> queryResources(String topoName, Integer projectTopoTableId) throws SQLException;

    List<Sink> querySinks(String topoName) throws SQLException;

    List<Sink> querySinks(String topoName, Integer projectTopoTableId) throws SQLException;

    List<EncodePlugin> queryEncodePlugins(String topoName) throws SQLException;

    Map<Long, List<DBusRouterEncodeColumn>> queryEncodeConfig(String topoName) throws SQLException;

    Map<Long, List<DBusRouterEncodeColumn>> queryEncodeConfig(String topoName, Integer projectTopoTableId) throws SQLException;

    Map<Long, List<FixColumnOutPutMeta>> queryFixColumnOutPutMeta(String topoName) throws SQLException;

    Map<Long, List<FixColumnOutPutMeta>> queryFixColumnOutPutMeta(String topoName, Integer projectTopoTableId) throws SQLException;

    int toggleProjectTopologyTableStatus(Integer id, String status) throws SQLException;

    int toggleProjectTopologyStatus(Integer id, String status) throws SQLException;

    int updateProjectTopologyConfig(Integer id, String config) throws SQLException;

    boolean isUsingTopic(Integer id) throws SQLException;

//    int updateSchemaChangeFlag(Long id, String tableName, int value) throws SQLException;

    int updateTpttSchemaChange(Long id, int value) throws SQLException;

    int updateTpttmvSchemaChange(Long id, int value, String changeComment) throws SQLException;

    int updateTptteocSchemaChange(Long id, int value, String changeComment) throws SQLException;
}
