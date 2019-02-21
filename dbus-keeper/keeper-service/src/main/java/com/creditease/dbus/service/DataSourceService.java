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

package com.creditease.dbus.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.alibaba.fastjson.JSON;
import com.creditease.dbus.bean.DataSourceValidateBean;
import com.creditease.dbus.bean.TopologyStartUpBean;
import com.creditease.dbus.commons.IZkService;
import com.creditease.dbus.constant.KeeperConstants;
import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.constant.TopologyType;
import com.creditease.dbus.domain.mapper.DataSchemaMapper;
import com.creditease.dbus.domain.mapper.DataSourceMapper;
import com.creditease.dbus.domain.mapper.DataTableMapper;
import com.creditease.dbus.domain.model.DataSource;
import com.creditease.dbus.enums.DbusDatasourceType;
import com.creditease.dbus.service.source.MongoSourceFetcher;
import com.creditease.dbus.service.source.SourceFetcher;
import com.creditease.dbus.utils.StormToplogyOpHelper;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;

import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * User: 王少楠
 * Date: 2018-05-07
 * Time: 下午5:34
 */
@Service
public class DataSourceService {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private DataSourceMapper mapper;

    @Autowired
    private DataSchemaMapper schemaMapper;

    @Autowired
    private DataTableMapper tableMapper;

    @Autowired
    private IZkService zkService;

    @Autowired
    private JarManagerService jarManagerService;

    /*数据源检查的结果 */
    private static final int VALIDATE_OK = 1;
    private static final int VALIDATE_ERROR_TYPE = 0;
    private static final int VALIDATE_ERROR = -1;

    public static final int DATASOURCE_EXISTS=-1;

    /**
     * resource页的搜索
     *
     * @param dsName 若为空，则返回所有DataSource信息
     * @return
     */
    public PageInfo<Map<String, Object>> search(int pageNum, int pageSize, String dsName,
                                                String sortBy, String order, String dsType) {
        Map<String, Object> param = new HashedMap();
        param.put("dsName", dsName==null?dsName:dsName.trim());
        param.put("sortBy", sortBy==null?sortBy:sortBy.trim());
        param.put("dsType", dsType==null?dsType:dsType.trim());
        param.put("order", order);

        PageHelper.startPage(pageNum, pageSize);
        List<Map<String, Object>> dataSources = mapper.search(param);
        try {
            // 获取各数据线Topology运行情况
            if (!StormToplogyOpHelper.inited) {
				StormToplogyOpHelper.init(zkService);
            }
            StormToplogyOpHelper.populateToposToDs(dataSources);
        } catch (Exception e) {
            logger.warn("Get Storm Rest API failed. Can't access Topology running status!");
        }
        return new PageInfo<>(dataSources);
    }

    /**
     * @param id 数据库中的primary  key
     */
    public DataSource getById(Integer id) {
        return mapper.selectByPrimaryKey(id);
    }

    /**
     * 根据 ID更新某条记录
     *
     * @param updateOne
     */
    public int update(DataSource updateOne) {
        updateOne.setUpdateTime(new Date());
        if(updateOne.getStatus().equals("inactive")){
            schemaMapper.inactiveSchemaByDsId(updateOne.getId());
            tableMapper.inactiveTableByDsId(updateOne.getId());
        }
        return mapper.updateByPrimaryKey(updateOne);
    }

    /**
     * @return 插入的新数据的ID
     */
    public int insertOne(DataSource newOne) {
        //根据dsName检查是否存在
        List<DataSource> oldDSList = getDataSourceByName(newOne.getDsName());
        if(oldDSList != null && oldDSList.size()>0){
            logger.info("[add dataSource] DataSource exists: {}",newOne.getDsName());
            return DATASOURCE_EXISTS;
        }
        newOne.setUpdateTime(new Date());
        mapper.insert(newOne);
        return newOne.getId();
    }

    /**
     * 根据ID直接从数据库里删除该条数据
     *
     * @param id
     */
    public int deleteById(Integer id) {
        return mapper.deleteByPrimaryKey(id);
    }


    /**
     * 根据数据源名称查询数据源
     *
     * @param dsName 数据源名称,不存在则查询所有数据源
     * @return 返回满足条件的数据源列表
     */
    public List<DataSource> getDataSourceByName(String dsName) {
        HashMap<String, Object> param = new HashMap<>();
        param.put("dsName", StringUtils.trim(dsName));
        return mapper.getDataSourceByName(param);
    }

    public void modifyDataSourceStatus(Long id, String status) {
        HashMap<String, Object> param = new HashMap<>();
        param.put("id", id);
        param.put("status", status);
        mapper.updateDsStatusByPrimaryKey(param);
    }

    public List<Map<String, Object>> getDSNames() {
        return mapper.getDSNames();
    }

    /**
     * 检查数据源的信息是否正确(能否连通)
     * @return MessageCode
     */
    public int validate(DataSourceValidateBean dsInfo){
        Map<String,Object> masterParam = buildValidateParam(dsInfo,true);//获取master的参数
        Map<String,Object> slaverParam = buildValidateParam(dsInfo,false);//获取slaver的参数
        try{
            int masterRes = doValidateParam(masterParam);
            int slaveRes = doValidateParam(slaverParam);

            if(masterRes == VALIDATE_OK && slaveRes == VALIDATE_OK){
                return MessageCode.OK;
            }else {
               return MessageCode.DATASOURCE_VALIDATE_FAILED;
            }
        }catch (Exception e){
            logger.error("[validate datasource info]dsInfo:{}, Exception: {}",dsInfo.toString(),e);
            throw e;
        }

    }

    private int doValidateParam(Map<String,Object> map){
        try {
            DbusDatasourceType dsType = DbusDatasourceType.parse(map.get("dsType").toString());
            if(dsType.equals(DbusDatasourceType.LOG_LOGSTASH)
                    || dsType.equals(DbusDatasourceType.LOG_LOGSTASH_JSON)
                    || dsType.equals(DbusDatasourceType.LOG_UMS)
                    || dsType.equals(DbusDatasourceType.LOG_FLUME)
                    || dsType.equals(DbusDatasourceType.LOG_FILEBEAT)) {
                return VALIDATE_OK;//1;
            }
            else if(dsType.equals(DbusDatasourceType.MYSQL)
                    || dsType.equals(DbusDatasourceType.ORACLE)
                    ){
                SourceFetcher fetcher = SourceFetcher.getFetcher(map);
                int temp = fetcher.fetchSource(map);
                return temp;
            } else if(dsType.equals(DbusDatasourceType.MONGO)) {
                MongoSourceFetcher fetcher = new MongoSourceFetcher();
                int temp = fetcher.fetchSource(map);
                return temp;
            }
            return VALIDATE_ERROR_TYPE;

        } catch (Exception e) {
            logger.error("[datasource validate param]Error encountered, parameter:{}", JSON.toJSONString(map), e);
            return VALIDATE_ERROR;
        }
    }


    /**
     * 构造检查参数
     * @param ifMaster master 或 slave
     */
    private Map<String,Object> buildValidateParam(DataSourceValidateBean rowParam,boolean ifMaster){
        Map<String,Object> validateParam = new HashedMap(4);
        validateParam.put("dsType",rowParam.getDsType());
        validateParam.put("user",rowParam.getUser());
        validateParam.put("password",rowParam.getPwd());
        //根据传入的不同类型,构造不同的结果
        if(ifMaster){
            validateParam.put("URL",rowParam.getMasterUrl());
        }else {
            validateParam.put("URL",rowParam.getSlaveUrl());
        }
        return validateParam;
    }

    /**
     * 添加数据源
     * @return dsId，根据dsID进行后续的操作。
     */
    public Integer addDataSource(DataSource newDS){
        mapper.insert(newDS);
        return newDS.getId();
    }

    public List<TopologyStartUpBean> getPath(int dsId){
        try {
            DataSource dataSource =getById(dsId);
            String dsType = dataSource.getDsType();
            //1.根据dsType--->得到需要启动的topologyType s
            //2.根据topologyType--->得到对应的jar信息列表
            //3.从jar信息列表中--->获取最新的jar版本
            //4.构造返回结果

            //获取storm path信息
            Properties globalConf = zkService.getProperties(KeeperConstants.GLOBAL_CONF);
            String stormStartScriptPath =  globalConf.getProperty(KeeperConstants.GLOBAL_CONF_KEY_STORM_START_SCRIPT_PATH);

            //根据dsType--->得到需要启动的topologyType列表
            List<TopologyType> topologyTypes = getTopologyTypesByDsType(dsType);
            if(topologyTypes == null){
                throw new Exception("datasource dsType error");
            }

            List<TopologyStartUpBean> result = new ArrayList<>(topologyTypes.size()); //定义接口返回的结果

            /*根据topologyType，得到相应的jar信息，然后构造返回信息*/
            for(TopologyType topologyType : topologyTypes ){
                List<Map<String, String>> jars = jarManagerService.queryJarInfos(null, null, topologyType.getValue());
                //将jar排序，然后获取最新的一个
                Collections.sort(jars, new Comparator<Map<String, String>>() {
                    @Override
                    public int compare(Map<String, String> jar1, Map<String, String> jar2) {
                        /*根据返回的jar的version信息来比较:
                        * 该字段的值是形如：0.4.x的类型的数据，比较前两位的数字即可
                        * */
                        float jar1Version = versionToFloat(jar1.get("version"));
                        float jar2Version = versionToFloat(jar2.get("version"));
                        float result = jar1Version-jar2Version;
                        if(result == 0){
                            return 0;
                        }else {
                            return result < 0 ? -1 : 1;
                        }
                    }
                });
                //根据排序结果，获取最新的jar信息，然后构造返回结果
                Map<String, String> latestJar = jars.get(jars.size()-1);
                TopologyStartUpBean topologyStartUpBean = new TopologyStartUpBean();
                topologyStartUpBean.setDsName(dataSource.getDsName());
                String topologyTypeStr = topologyType.getValue().replace('_','-'); //将路径的的下划线换成名称的短横线
                topologyStartUpBean.setTopolotyType(topologyTypeStr);
                topologyStartUpBean.setTopolotyName(dataSource.getDsName()+'-'+topologyTypeStr);
                topologyStartUpBean.setStatus("inactive");
                topologyStartUpBean.setStormPath(stormStartScriptPath);
                topologyStartUpBean.setJarName(latestJar.get("fileName"));
                topologyStartUpBean.setJarPath(latestJar.get("path"));
                //加入返回结果集
                result.add(topologyStartUpBean);
            }

            return result;
        }catch (Exception e){
            logger.error("[get path] DsId:{}, Exception:{}",dsId,e);
            return null;
        }
    }

    private static Float versionToFloat(String version){
        String digitalStr =version.substring(0,version.lastIndexOf("."));
        float digital = Float.valueOf(digitalStr);
        return digital;
    }

    /**
     * 根据数据源的dsType，判断需要返回的type信息
     */
    private List<TopologyType> getTopologyTypesByDsType(String dsType){
        DbusDatasourceType dbusDatasourceType =DbusDatasourceType.parse(dsType);
        List<TopologyType> topologyTypes = new ArrayList<>();
        if(dbusDatasourceType == DbusDatasourceType.MYSQL){
            topologyTypes.add(TopologyType.DISPATCHER_APPENDER);
            topologyTypes.add(TopologyType.SPLITTER_PULLER);
            topologyTypes.add(TopologyType.MYSQL_EXTRACTOR);

        } else if (dbusDatasourceType == DbusDatasourceType.ORACLE || dbusDatasourceType == DbusDatasourceType.MONGO) {
            topologyTypes.add(TopologyType.DISPATCHER_APPENDER);
            topologyTypes.add(TopologyType.SPLITTER_PULLER);
        }
        else if(dbusDatasourceType ==DbusDatasourceType.LOG_FILEBEAT ||
                dbusDatasourceType == DbusDatasourceType.LOG_FLUME ||
                dbusDatasourceType == DbusDatasourceType.LOG_LOGSTASH ||
                dbusDatasourceType == DbusDatasourceType.LOG_LOGSTASH_JSON ||
                dbusDatasourceType == DbusDatasourceType.LOG_UMS){
            topologyTypes.add(TopologyType.LOG_PROCESSOR);
        }else {
            return null;
        }
        return topologyTypes;
    }

    public DataSource getByName(String dsName){
        return mapper.getByName(StringUtils.trim(dsName));
    }

    public List<DataSource> getDataSourceByDsType(String dsType) {
        return mapper.getDataSourceByDsType(dsType);
    }

    public List<DataSource> getDataSourceByDsTypes(List<String> dsTypes) {
        return mapper.getDataSourceByDsTypes(dsTypes);
    }

}
