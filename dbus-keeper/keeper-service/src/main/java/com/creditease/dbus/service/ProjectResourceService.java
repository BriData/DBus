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

import com.creditease.dbus.domain.mapper.ProjectResourceMapper;
import com.creditease.dbus.domain.mapper.ProjectTopoTableMapper;
import com.creditease.dbus.domain.model.ProjectResource;
import com.creditease.dbus.domain.model.ProjectTopoTable;
import com.creditease.dbus.domain.model.TableStatus;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import org.apache.commons.collections.map.HashedMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * Created by mal on 2018/3/27.
 */
@Service
public class ProjectResourceService {

    @Autowired
    private ProjectResourceMapper mapper;

    @Autowired
    private ProjectTopoTableMapper tableMapper;

    public int insert(ProjectResource projectResource) {
        return mapper.insert(projectResource);
    }

    public void insertAll(List<ProjectResource> projectResources) {
        for (ProjectResource projectResource : projectResources) {
            insert(projectResource);
        }
    }

    public List<Map<String, Object>> selectByProjectId(int id) {
        return mapper.selectByProjectId(id);
    }

    public int update(ProjectResource projectResource) {
        return mapper.updateByPrimaryKey(projectResource);
    }

    public void updateAll(List<ProjectResource> projectResources) {
        for (ProjectResource projectResource : projectResources) {
            update(projectResource);
        }
    }

    public int deleteByProjectId(int id) {
        return mapper.deleteByProjectId(id);
    }

    /**
     * 查询条件为空,则返回所有数据
     *
     * @param projectId 精确匹配(=)
     * @return
     */
    public PageInfo<Map<String, Object>> queryResource(String dsName,
                                                       String schemaName,
                                                       String tableName,
                                                       Integer pageNum,
                                                       Integer pageSize,
                                                       Integer projectId) {
        Map<String, Object> param = new HashedMap();
        param.put("dsName", dsName == null ? dsName : dsName.trim());
        param.put("schemaName", schemaName == null ? schemaName : schemaName.trim());
        param.put("tableName", tableName == null ? tableName : tableName.trim());
        param.put("projectId", projectId);

        PageHelper.startPage(pageNum, pageSize);
        //查询出基本信息,datasource,schema,table的name等信息
        List<Map<String, Object>> resources = mapper.search(param);

        //拼接其他状态字段
        for (Map<String, Object> resource : resources) {
            int tableId = ((Long) resource.get("tableId")).intValue();
            int resourceProjectId = ((Long) resource.get("projectId")).intValue();//传入的projectId可能为null
            //查询status,用"use"和"unuse"替换,"OK"和"abort"
            String status = mapper.searchStatus(resourceProjectId, tableId);
            resource.put("status", status);
            //脱敏要求
            String encodeRequest = mapper.searchEncodeType(tableId, projectId);
            resource.put("mask", encodeRequest);
            //是否投产
            boolean ifRunning = checkRunningStatus(tableId);
            resource.put("running", ifRunning);

        }

        return new PageInfo(resources);
    }

    /**
     * 根据sourcetableId 判断该resource是否running
     */
    private boolean checkRunningStatus(Integer sourceTableId) {
        List<ProjectTopoTable> projectTopoTables = tableMapper.selectBySourceTableId(sourceTableId);
        for (ProjectTopoTable table : projectTopoTables) {
            if (TableStatus.RUNNING.getValue().equals(table.getStatus())) {
                return true;
            }
        }
        return false;
    }

    /**
     * 查询resource的使用状态
     *
     * @param projectId
     * @param tableId
     * @return "use" or "unuse"
     */
    public String checkStatus(int projectId, int tableId) {
        return mapper.searchStatus(projectId, tableId);
    }

    /**
     * 查询条件为空,则返回所有数据
     *
     * @param projectId 精确匹配(=)
     * @return
     */
    public PageInfo<Map<String, Object>> queryProjectResource(String dsName,
                                                              String schemaName,
                                                              String tableName,
                                                              Integer pageNum,
                                                              Integer pageSize,
                                                              Integer projectId) {
        Map<String, Object> param = new HashedMap();
        param.put("dsName", dsName == null ? dsName : dsName.trim());
        param.put("schemaName", schemaName == null ? schemaName : schemaName.trim());
        param.put("tableName", tableName == null ? tableName : tableName.trim());
        param.put("projectId", projectId);

        PageHelper.startPage(pageNum, pageSize);

        //查询出基本信息,datasource,schema,table的name等信息
        List<Map<String, Object>> resources = mapper.search(param);

        //去除基本查询多余的字段
        for (Map<String, Object> resource : resources) {
            resource.remove("projectName");
            int tableId = ((Long) resource.get("tableId")).intValue();
            //脱敏要求
            String encodeRequest = mapper.searchEncodeType(tableId, projectId);
            resource.put("mask", encodeRequest);
        }

        return new PageInfo(resources);
    }

    /**
     * 获取当前project下,Resource对应的DataSource的names集合,供下拉列表
     *
     * @param projectId 可选参数,如果 projectId==null,则返回所有Resource下的dsname.在外层的项目管理中使用
     * @return
     */
    public List<Map<String, Object>> getDatasourceNames(Integer projectId) {
        return mapper.getDSNames(projectId);
    }

    public List<Map<String, Object>> getProjectNames() {
        return mapper.getProjectNames();
    }

    /**
     * 根据ProjectId和tableId获取唯一的ProjectResource信息
     *
     * @param projectId
     * @param tableId
     * @return ProjectResource
     */
    public ProjectResource getByPIdAndTId(Integer projectId, Integer tableId) {
        return mapper.selectByPIdTId(projectId, tableId);
    }

}
