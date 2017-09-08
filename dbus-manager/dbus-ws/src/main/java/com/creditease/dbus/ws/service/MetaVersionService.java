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
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;

import java.util.List;
import java.util.Map;

/**
 * Created by zhangyf on 2016/10/12.
 */
public class MetaVersionService {
    private MybatisTemplate template = MybatisTemplate.template();

    /**
     * service工厂方法
     *
     * @return 返回TablesService实例
     */
    public static MetaVersionService getService() {
        return new MetaVersionService();
    }

    public PageInfo<TableVersion> search(int startPage, int pageSize, Map<String, Object> param) {
        PageHelper.startPage(startPage, pageSize);
        List<TableVersion> list = template.query((session, args) -> {
            TableVersionMapper mapper = session.getMapper(TableVersionMapper.class);
            // 设置分页
            return mapper.search(param);
        });
        // 分页结果
        PageInfo<TableVersion> page = new PageInfo<>(list);
        return page;
    }
}
