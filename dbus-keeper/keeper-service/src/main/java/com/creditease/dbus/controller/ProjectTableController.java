/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2018 Bridata
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

package com.creditease.dbus.controller;

import java.util.List;
import java.util.Map;

import com.creditease.dbus.base.BaseController;
import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.domain.model.ProjectTopoTable;
import com.creditease.dbus.domain.model.ProjectTopoTableEncodeOutputColumns;
import com.creditease.dbus.domain.model.ProjectTopoTableMetaVersion;
import com.creditease.dbus.service.ProjectTableService;
import com.github.pagehelper.PageInfo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Created with IntelliJ IDEA
 * Description:
 * User: 王少楠
 * Date: 2018-04-13
 * Time: 下午6:26
 */
@RestController
@RequestMapping("/projectTable")
public class ProjectTableController extends BaseController{

    @Autowired
    private ProjectTableService tableService;

    @GetMapping("/search-table")
    /**
     * table 首页的搜索
     */
    public ResultEntity searchTable(String dsName,
                                    String schemaName,
                                    String tableName,
                                    @RequestParam(defaultValue = "1") Integer pageNum,
                                    @RequestParam(defaultValue = "10")Integer pageSize,
                                    Integer projectId,
                                    Integer topoId){

        PageInfo<Map<String,Object>> page = tableService.queryTable(dsName,schemaName,tableName,pageNum,pageSize,projectId,topoId);
        return resultEntityBuilder().payload(page).build();
    }

    @GetMapping("/search-table-nopage")
    public ResultEntity searchTable(String dsName, String schemaName, String tableName, Integer projectId, Integer topoId) {
        List<Map<String, Object>> topoTables = tableService.queryTable(dsName, schemaName, tableName, projectId, topoId);
        return resultEntityBuilder().payload(topoTables).build();
    }

    @PostMapping("/search-tables")
    public ResultEntity searchTable(@RequestBody List<Integer> topoTableIds) {
        List<Map<String, Object>> topoTables = tableService.queryTable(topoTableIds);
        return resultEntityBuilder().payload(topoTables).build();
    }


    /**
     * 添加table页的resource搜索项
     */
    @GetMapping("/search-resource")
    public ResultEntity searchResource(String dsName,
                                    String schemaName,
                                    String tableName,
                                    @RequestParam(defaultValue = "1") Integer pageNum,
                                    @RequestParam(defaultValue = "10")Integer pageSize,
                                    @RequestParam Integer projectId,
                                       @RequestParam Integer topoId,
                                       @RequestParam(required = false) Integer hasDbaEncode){

        PageInfo<Map<String,Object>> page = tableService.queryResource(dsName,schemaName,tableName,
                pageNum,pageSize,projectId,topoId,hasDbaEncode);
        return resultEntityBuilder().payload(page).build();
    }

    @GetMapping("/topology-names")
    /**
     * 选择topology的下拉列表
     */
    public ResultEntity  getTopologyNames(Integer projectId){
        return resultEntityBuilder().payload(tableService.getTopologyNames(projectId)).build();
    }

    @GetMapping("/project-topologies")
    /**
     * 获取project下所有topo
     */
    public ResultEntity  getProjectTopologies(Integer projectId){
        return resultEntityBuilder().payload(tableService.getProjectTopologies(projectId)).build();
    }

    @GetMapping("/project-names")
    /**
     * 选择project的下拉列表
     */
    public ResultEntity getProjectNames(){
        return resultEntityBuilder().payload(tableService.getProjectNames()).build();
    }

    /**
     * DataSource 下拉列表
     */
    @GetMapping("/datasource-names")
    public ResultEntity getDSNames(Integer projectId){
        return resultEntityBuilder().payload(tableService.getDSNames(projectId)).build();
    }

    @GetMapping("/delete-by-table-id/{id}/{topoStatus}")
    public ResultEntity deleteByTableId(@PathVariable int id,@PathVariable String topoStatus){
        return resultEntityBuilder().status(tableService.deleteByTableId(id, topoStatus)).build();
    }

    @GetMapping("/delete-column-by-table-id/{id}")
    public ResultEntity deleteColumnByTableId(@PathVariable int id){
        return resultEntityBuilder().payload(tableService.deleteColumnByTableId(id)).build();
    }

    @PostMapping("/insert")
    public ResultEntity insert(@RequestBody ProjectTopoTable table) {
        int id = tableService.insert(table);
        if(id == ProjectTableService.TABLE_EXIST){
            return resultEntityBuilder().status(MessageCode.TABLE_ALREADY_EXISTS).build();
        }
        return resultEntityBuilder().payload(id).build();
    }

    @PostMapping("/insertColumns")
    public ResultEntity insertColumns(@RequestBody List<ProjectTopoTableEncodeOutputColumns> columnsList){
        tableService.insertColumns(columnsList);
        return resultEntityBuilder().build();
    }

    /**
     * @return 数据格式：{
     *     sink:{"sinkId":1,"ouputType":json/ums1.1","outputTopic":"test2test"},
     *     resource:{"schemaName":"xx",..,"topoId":1},
     *     "encodes":{
     *                "1":{"outputListType":"1","encodeOutputColumns":[
     *                        {"fieldName":"a","encodeType":"type","encodeParam":"1","truncate":"1"}
     *                        {"fieldName":"b","encodeType":"type","encodeParam":"1","truncate":"1"}
     *                           ]
     *                    }
     *      }
     */
    @GetMapping("/select/{projectId}/{projectTableId}")
    public ResultEntity queryById(@PathVariable Integer projectId,@PathVariable Integer projectTableId){
        try {
            Map<String, Object> result = tableService.queryById(projectId, projectTableId);
            if(result == null){
                return resultEntityBuilder().status(MessageCode.TABLE_DATA_FORMAT_ERROR).build();
            }else {
                return resultEntityBuilder().payload(result).build();
            }
        }catch (Exception e){
            return resultEntityBuilder().status(Integer.valueOf(e.getMessage())).build();
        }

    }

    /**
     * 获取用户自定义脱敏的列的信息
     */
    /*@GetMapping("/encode-columns/{tableId}")
    public ResultEntity getOutputEncodeColumns(@PathVariable Integer tableId){
        return resultEntityBuilder().payload(
                tableService.getOutputEncodeColumns(tableId)
        ).build();
    }*/

    @PostMapping(value = "/update",consumes = "application/json")
    public ResultEntity updateTable(@RequestBody ProjectTopoTable table){
        int tableId = tableService.updateTable(table);
        if(tableId == ProjectTableService.TABLE_NOT_FOUND){
            return resultEntityBuilder().status(MessageCode.TABLE_NOT_EXISTS).build();
        }
        return resultEntityBuilder().payload(tableId).build();
    }

    @PostMapping(value = "/update-encode-columns",consumes = "application/json")
    public ResultEntity updateEncodeColumns(@RequestBody List<ProjectTopoTableEncodeOutputColumns> columnList){
        tableService.updateColumns(columnList);
        return resultEntityBuilder().build();
    }

    @GetMapping("/partition-offset")
    public ResultEntity getPartitionMsgs(@RequestParam String topic){
        return resultEntityBuilder().payload(
                tableService.getTopicOffsets(topic)
                ).build();
    }

    @GetMapping("/affected-tables")
    public ResultEntity getAffectedTables(@RequestParam String topic,@RequestParam Integer tableId){
        return resultEntityBuilder().payload(
                tableService.getTableNamesByTopic(topic,tableId)
                ).build(  );
    }

    @GetMapping("/table/{tableId}")
    public ResultEntity getTableById(@PathVariable Integer tableId){
        return resultEntityBuilder().payload(tableService.findTableById(tableId)).build();
    }

    @PostMapping("/meta-versions")
    public ResultEntity insertMetaVersions(@RequestBody List<ProjectTopoTableMetaVersion> metaVersions){
        tableService.insertOrUpdateMetaVersions(metaVersions);
        return resultEntityBuilder().build();
    }

    @PostMapping("/meta-versions-update")
    public ResultEntity updateMetaVersions(@RequestBody List<ProjectTopoTableMetaVersion> metaVersions){
        tableService.updateMetaVersions(metaVersions);
        return resultEntityBuilder().build();
    }

    @GetMapping("/count-by-ds-id/{dsId}")
    public ResultEntity countByDsId(@PathVariable Integer dsId){
        return resultEntityBuilder().payload(tableService.countByDsId(dsId)).build();
    }

    @GetMapping("/count-by-schema-id/{schemaId}")
    public ResultEntity countBySchemaId(@PathVariable Integer schemaId){
        return resultEntityBuilder().payload(tableService.countBySchemaId(schemaId)).build();
    }

    @GetMapping("/count-by-table-id/{tableId}")
    public ResultEntity countByTableId(@PathVariable Integer tableId){
        return resultEntityBuilder().payload(tableService.countByTableId(tableId)).build();
    }

    @GetMapping("/getTopoTablesByUserId/{userId}")
    public ResultEntity getTopoTablesByUserId(@PathVariable Integer userId){
        return resultEntityBuilder().payload(tableService.getTopoTablesByUserId(userId)).build();
    }

    @GetMapping("/getAllResourcesByQuery")
    public ResultEntity getAllResourcesByQuery(String dsName, String schemaName, String tableName,
                                               @RequestParam Integer projectId, @RequestParam Integer topoId ) {
        return resultEntityBuilder().payload(tableService.getAllResourcesByQuery(dsName, schemaName, tableName, projectId, topoId)).build();
    }

    @GetMapping("/uottcisp/{projectId}/{tableId}/{topoId}")
    public ResultEntity underOtherTopologyTableCountInSameProject(@PathVariable Integer projectId,
                                                                  @PathVariable Integer tableId,
                                                                  @PathVariable Integer topoId ) {
        return resultEntityBuilder().payload(tableService.underOtherTopologyTableCountInSameProject(projectId, tableId, topoId)).build();
    }

    /**
     * 根据topoTableId获取
     * ds_name,schema_name,table_name, project_name,topo_name
     */
    @GetMapping("/getNamesByTopoTableId/{topoTableId}")
    public ResultEntity getNamesByTopoTableId(@PathVariable Integer topoTableId) {
        return resultEntityBuilder().payload(tableService.getNamesByTopoTableId(topoTableId)).build();
    }

    @PostMapping("/getTopoTablesByIds")
    public ResultEntity getTopoTablesByIds(@RequestBody List<Integer> topoTableIds) {
        return resultEntityBuilder().payload(tableService.getTopoTablesByIds(topoTableIds)).build();
    }

    @PostMapping("/updateStatusByTopoTableIds")
    public ResultEntity updateStatusByTopoTableIds(@RequestParam String status, @RequestBody List<Integer> topoTableIds) {
        tableService.updateStatusByTopoTableIds(status, topoTableIds);
        return resultEntityBuilder().build();
    }

}
