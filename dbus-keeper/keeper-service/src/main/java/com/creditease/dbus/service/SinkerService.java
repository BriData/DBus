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

import com.creditease.dbus.domain.mapper.SinkerTopologyMapper;
import com.creditease.dbus.domain.model.SinkerTopology;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by mal on 2018/3/23.
 */
@Service
public class SinkerService {
    protected Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private SinkerTopologyMapper sinkerTopologyMapper;

    public PageInfo<SinkerTopology> search(int pageNum, int pageSize, String sinkerName, String sortby, String order) {
        Map<String, Object> map = new HashMap<>();
        map.put("sortby", sortby);
        map.put("order", order);
        if (StringUtils.isNotBlank(sinkerName)) {
            map.put("sinkerName", sinkerName);
        }
        PageHelper.startPage(pageNum, pageSize);
        return new PageInfo(sinkerTopologyMapper.search(map));
    }

    public int create(SinkerTopology sinkerTopology) {
        sinkerTopology.setStatus("new");
        sinkerTopology.setUpdateTime(new Date());
        return sinkerTopologyMapper.insert(sinkerTopology);
    }

    public int update(SinkerTopology sinkerTopology) {
        sinkerTopology.setUpdateTime(new Date());
        return sinkerTopologyMapper.updateByPrimaryKey(sinkerTopology);
    }

    public int delete(Integer id) {
        return sinkerTopologyMapper.deleteByPrimaryKey(id);
    }

    public SinkerTopology searchBySinkerName(String sinkerName) {
        return sinkerTopologyMapper.searchBySinkerName(sinkerName);
    }

    public SinkerTopology searchById(Integer id) {
        return sinkerTopologyMapper.selectByPrimaryKey(id);
    }

}
