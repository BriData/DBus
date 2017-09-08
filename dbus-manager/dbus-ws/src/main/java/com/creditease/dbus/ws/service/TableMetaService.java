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

import com.creditease.dbus.ws.domain.TableMeta;
import com.creditease.dbus.ws.mapper.TableMetaMapper;
import com.creditease.dbus.ws.service.mybatis.MybatisTemplate;

public class TableMetaService {
    private MybatisTemplate template = MybatisTemplate.template();
    public static TableMetaService getService() {
        return new TableMetaService();
    }

    public int insertMetas(TableMeta tm) {
        return template.update((session, args) -> {
            TableMetaMapper tableMetaMapper = session.getMapper(TableMetaMapper.class);
            return tableMetaMapper.insert(tm);
        });
    }
}
