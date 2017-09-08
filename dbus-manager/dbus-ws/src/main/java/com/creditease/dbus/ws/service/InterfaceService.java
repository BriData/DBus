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

import com.creditease.dbus.ws.domain.InterfaceInfo;
import com.creditease.dbus.ws.mapper.InterfaceMapper;
import com.creditease.dbus.ws.service.mybatis.MybatisTemplate;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class InterfaceService {
    private MybatisTemplate template = MybatisTemplate.template();

    /**
     * service工厂方法
     *
     * @return 返回InterfaceService实例
     */
    public static InterfaceService getService() {
        return new InterfaceService();
    }

    public List<InterfaceInfo> searchInfo() {
        List<InterfaceInfo> list = template.query((session, args) -> {
            InterfaceMapper mapper = session.getMapper(InterfaceMapper.class);
            return mapper.searchInfo();
        });
        return list;
    }
}
