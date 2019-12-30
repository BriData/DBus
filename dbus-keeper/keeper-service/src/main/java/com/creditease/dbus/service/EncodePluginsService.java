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


package com.creditease.dbus.service;

import com.creditease.dbus.domain.mapper.EncodePluginsMapper;
import com.creditease.dbus.domain.model.EncodePlugins;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * User: 王少楠
 * Date: 2018-06-08
 * Time: 下午3:31
 */
@Service
public class EncodePluginsService {
    @Autowired
    private EncodePluginsMapper mapper;

    public List<EncodePlugins> getProjectEncodePlugins(int projectId) {
        return mapper.selectByProject(projectId);
    }

    public void create(EncodePlugins encodePlugin) {
        List<EncodePlugins> encodePlugins = mapper.selectAll();
        if (encodePlugins.size() == 0) {
            mapper.insert(encodePlugin);
        }
    }

}
