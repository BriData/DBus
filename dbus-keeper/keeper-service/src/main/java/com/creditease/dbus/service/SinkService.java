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

import com.creditease.dbus.domain.mapper.ProjectSinkMapper;
import com.creditease.dbus.domain.mapper.SinkMapper;
import com.creditease.dbus.domain.mapper.SinkerTopologyMapper;
import com.creditease.dbus.domain.mapper.SinkerTopologySchemaMapper;
import com.creditease.dbus.domain.model.Sink;
import com.creditease.dbus.domain.model.SinkerTopology;
import com.creditease.dbus.domain.model.SinkerTopologySchema;
import com.creditease.dbus.utils.DBusUtils;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by mal on 2018/3/23.
 */
@Service
public class SinkService {

    @Autowired
    private SinkMapper mapper;
    @Autowired
    private ProjectSinkMapper projectSinkMapper;


    protected Logger logger = LoggerFactory.getLogger(getClass());

    public PageInfo<Sink> search(Sink sink, int pageNum, int pageSize, String sortby, String order) {
        Map<String, Object> map = DBusUtils.object2map(sink);
        map.put("sortby", sortby);
        map.put("order", order);
        PageHelper.startPage(pageNum, pageSize);
        return new PageInfo(mapper.search(map));
    }

    public void createSink(Sink sink) {
        mapper.insert(sink);
    }

    public void updateSink(Sink sink) {
        mapper.updateByPrimaryKey(sink);
    }

    public void deleteSink(Integer id) {
        mapper.deleteByPrimaryKey(id);
    }

    public Sink getSink(String sinkName, Integer id) {
        return mapper.searchBySinkName(sinkName, id);
    }

    public int getProjectBySinkId(Integer id) {
        return projectSinkMapper.getBySinkId(id);
    }

    public Sink getSinkById(Integer id) {
        return mapper.selectByPrimaryKey(id);
    }

    public PageInfo<Sink> search(Integer pageNum, Integer pageSize, Integer userId, Integer projectId) {
        PageHelper.startPage(pageNum, pageSize);
        return new PageInfo(mapper.searchByUserProject(userId, projectId));
    }

    public void exampleSink(Sink sink) {
        //已经初始化
        Sink s = getSink(sink.getSinkName(), null);
        if (s != null) {
            sink.setId(s.getId());
            mapper.updateByPrimaryKey(sink);
        } else {
            mapper.insert(sink);
        }
    }

}
