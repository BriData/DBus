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

import com.creditease.dbus.ws.domain.FullPullHistory;
import com.creditease.dbus.ws.mapper.FullPullHistoryMapper;
import com.creditease.dbus.ws.service.mybatis.MybatisTemplate;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;

import java.util.List;
import java.util.Map;

public class FullpullHistoryService {
    private MybatisTemplate template = MybatisTemplate.template();

    /**
     * service工厂方法
     * @return 返回 FullpullHistoryService 实例
     */
    public static FullpullHistoryService getService() {
        return new FullpullHistoryService();
    }

    public PageInfo<FullPullHistory> search(int pageNum, int pageSize, Map<String, Object> param) {
        PageHelper.startPage(pageNum, pageSize);
        List<FullPullHistory> list = template.query((session, args) -> {
            FullPullHistoryMapper mapper = session.getMapper(FullPullHistoryMapper.class);
            // 设置分页
            return mapper.search(param);
        });
        // 分页结果
        PageInfo<FullPullHistory> page = new PageInfo<>(list);
        return page;
    }
}
