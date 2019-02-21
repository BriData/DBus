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

import com.alibaba.fastjson.JSON;
import com.creditease.dbus.base.BaseController;
import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.bean.DataSourceValidateBean;
import com.creditease.dbus.bean.TopologyStartUpBean;
import com.creditease.dbus.commons.EncrypAES;
import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.domain.model.DataSource;
import com.creditease.dbus.service.DataSourceService;
import com.creditease.dbus.service.source.SourceFetcher;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * User: 王少楠
 * Date: 2018-05-07
 * Time: 下午5:30
 */
@RestController
@RequestMapping("/datasource")
public class DataSourceController extends BaseController{

    @Autowired
    private DataSourceService service;

    /**
     * resource页的搜索
     * @param dsName 可以为空，如此，则返回所有
     * @param sortBy 排序的关键字，默认为updateTime
     * @param order asc/desc 默认是asc（增序）
     * @return
     */
    @GetMapping("/search")
    public ResultEntity search(@RequestParam(defaultValue = "1") Integer pageNum,
                               @RequestParam(defaultValue = "10") Integer pageSize,
                               String dsName,String sortBy,String order,String dsType){
        if (!StringUtils.isBlank(order)) {
            if (!order.equalsIgnoreCase("asc") && !order.equalsIgnoreCase("desc")) {
                order = "desc";
            }
        }
        return resultEntityBuilder().payload(service.search(pageNum,pageSize,dsName,sortBy,order,dsType)).build();
    }

    @GetMapping("/{id}")
    public ResultEntity getById(@PathVariable Integer id){
        return resultEntityBuilder().payload(service.getById(id)).build();
    }

    @PostMapping("")
    public ResultEntity addOne(@RequestBody DataSource newOne){
        int dsId = service.insertOne(newOne);
        if(dsId==service.DATASOURCE_EXISTS){
            return resultEntityBuilder().status(MessageCode.DATASOURCE_ALREADY_EXISTS).build();
        }else {
            return resultEntityBuilder().payload(dsId).build();
        }
    }

    @GetMapping("/delete/{id}")
    public ResultEntity deleteById(@PathVariable Integer id){
        return resultEntityBuilder().payload(service.deleteById(id)).build();
    }

    @PostMapping("/update")
    public ResultEntity updateById(@RequestBody DataSource updateOne){
        return resultEntityBuilder().payload(service.update(updateOne)).build();
    }

    /**
     * 根据数据源名称获取数据源列表。
     * 旧接口:findDataSources(@QueryParam("name") String name)
     * @param name 如果name不存在则返回全部数据源
     * @return 数据源列表
     */
    @GetMapping("/getDataSourceByName")
    public ResultEntity getDataSourceByName(@RequestParam(required = false) String name){
        List<DataSource> list = service.getDataSourceByName(name);
        list.stream().forEach(ds->encryp(ds));
        return resultEntityBuilder().payload(list).build();
    }
    @GetMapping("/getDSNames")
    public ResultEntity getDSNames(){
        return resultEntityBuilder().payload(service.getDSNames()).build();
    }

    /**
     * 源端数据访问
     */
    @PostMapping("/searchFromSource")
    public ResultEntity searchFromSource(@RequestBody Map<String,Object> map){
        try {
            SourceFetcher fetcher = SourceFetcher.getFetcher(map);
            List list = fetcher.fetchTableStructure(map);
            return resultEntityBuilder().payload(list).build();
        } catch (Exception e) {
            logger.error("Error encountered while validate datasources with parameter:{}", JSON.toJSONString(map), e);
            return resultEntityBuilder().status(MessageCode.DATASOURCE_SOURCE_QUERY_FAILED).build();
        }
    }

    @PostMapping("/validate")
    public ResultEntity validateDataSources(@RequestBody DataSourceValidateBean dataSourceInfo){
        try {
            return resultEntityBuilder().status(service.validate(dataSourceInfo)).build();
        }catch (Exception e){
            return resultEntityBuilder().status(MessageCode.DATASOURCE_VALIDATE_FAILED).build();
        }
    }

    /**
     * 激活指定的数据源,使该数据源生效
     * @param id 数据源ID
     */
    @PostMapping("{id:[0-9]+}/{status}")
    public ResultEntity modifyDataSourceStatus(@PathVariable Long id, @PathVariable String status) {
        try {
            service.modifyDataSourceStatus(id, status);
            return resultEntityBuilder().payload("OK").build();
        } catch (Exception ex) {
            logger.error("Error encountered while active DateSource with parameter:{}", id, ex);
            return resultEntityBuilder().status(MessageCode.DATASOURCE_CHANGE_STATUS_FAILED).build();
        }
    }

    private static void encryp(DataSource ds) {
        if(ds != null && ds.getDbusPwd() != null) {
            ds.setDbusPwd(EncrypAES.encrypt(ds.getDbusPwd()));
        }
    }

    @GetMapping("/topologies-jars")
    public ResultEntity getPath(@RequestParam Integer dsId){
        List<TopologyStartUpBean> paths =service.getPath(dsId);
        if(paths == null){
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }else {
            return resultEntityBuilder().payload(paths).build();
        }
    }

    @GetMapping("/getByName")
    public ResultEntity getByName(String dsName) {
        return resultEntityBuilder().payload(service.getByName(dsName)).build();
    }

    /**
     * 根据传入的dsType列表查询dataSource
     * @param dsTypes
     * @return
     */
    @PostMapping("/getDataSourceByDsTypes")
    public ResultEntity getDataSourceByDsTypes(@RequestBody List<String> dsTypes) {
        return resultEntityBuilder().payload(service.getDataSourceByDsTypes(dsTypes)).build();
    }

    /**
     * 根据传入的dsType 模糊查询dataSource
     * @param dsType
     * @return
     */
    @GetMapping("/getDataSourceByDsType")
    public ResultEntity getDataSourceByDsType(@RequestParam String dsType) {
        return resultEntityBuilder().payload(service.getDataSourceByDsType(dsType)).build();
    }

}
