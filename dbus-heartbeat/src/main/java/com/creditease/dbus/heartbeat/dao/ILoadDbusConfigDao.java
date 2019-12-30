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


package com.creditease.dbus.heartbeat.dao;

import com.creditease.dbus.heartbeat.vo.*;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface ILoadDbusConfigDao {

    Map<String, String> queryAliasMapping(String key);

    List<DsVo> queryDsConfig(String key);

    Set<MonitorNodeVo> queryMonitorNode(String key);

    Set<TargetTopicVo> queryTargetTopic(String key);

    Set<ProjectMonitorNodeVo> queryProjectMonitorNode(String key);

    ProjectNotifyEmailsVO queryNotifyEmails(String key, String projectName);

    List<ProjectNotifyEmailsVO> queryRelatedNotifyEmails(String key, String datasource, String schema, String table);
}
