/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2017 Bridata
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

package com.creditease.dbus.ws.service;

import com.creditease.dbus.ws.domain.TableVersion;
import com.creditease.dbus.ws.mapper.TableVersionMapper;
import com.creditease.dbus.ws.service.mybatis.MybatisTemplate;

/**
 * Created by Shrimp on 16/9/8.
 */
public class TableVersionService {

    private MybatisTemplate template = MybatisTemplate.template();
    public static TableVersionService getService() {
        return new TableVersionService();
    }

    public TableVersion findById(long id) {
        return template.query(((session, args) -> {
            TableVersionMapper mapper = session.getMapper(TableVersionMapper.class);
            return mapper.findById(id);
        }));
    }

    public int updateVersion(long id, TableVersion v) {
        return template.update(((session, args) -> {
            TableVersionMapper mapper = session.getMapper(TableVersionMapper.class);
            v.setId(id);
            return mapper.update(v);
        }));
    }

    public int insertVersion(TableVersion v) {
        return template.update(((session, args) -> {
            TableVersionMapper mapper = session.getMapper(TableVersionMapper.class);
            return mapper.insert(v);
        }));
    }
}
