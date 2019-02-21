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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.ControlMessage;
import com.creditease.dbus.commons.ControlType;
import com.creditease.dbus.commons.IZkService;
import com.creditease.dbus.constant.KeeperConstants;
import com.creditease.dbus.constant.ProjectRemainTimeType;
import com.creditease.dbus.constant.ProjectStatus;
import com.creditease.dbus.domain.mapper.*;
import com.creditease.dbus.domain.model.Project;
import com.creditease.dbus.domain.model.ProjectTopoTable;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import static com.creditease.dbus.constant.KeeperConstants.CONF_KEY_GLOBAL_EVENT_TOPIC;
import static com.creditease.dbus.constant.ProjectRemainTimeType.ONE_DAY;

/**
 * Created by mal on 2018/3/26.
 */
@Service
public class ProjectService {

    @Autowired
    private ProjectMapper mapper;
    @Autowired
    private ProjectUserMapper projectUserMapper;
    @Autowired
    private ProjectTopoTableMapper projectTopoTableMapper;
    @Autowired
    private IZkService zkService;
    @Autowired
    private ProjectTopoTableEncodeOutputColumnsMapper encodeColumnsMapper;
    @Autowired
    private ProjectTopoTableMetaVersionMapper metaVersionMapper;
    @Autowired
    private ProjectEncodeHintMapper encodeHintMapper;
    @Autowired
    private ProjectResourceMapper resourceMapper;



    private Logger logger = LoggerFactory.getLogger(getClass());

    public PageInfo<Map<String, Object>> queryResources(String dsName,
                                         String schemaName,
                                         String tableName,
                                         int pageNum,
                                         int pageSize,
                                         String sortby,
                                         String order,
                                         Integer hasDbaEncode) {
        Map<String, Object> param = new HashMap<>();
        param.put("dsName", dsName);
        param.put("schemaName", schemaName);
        param.put("tableName", tableName);
        param.put("sortby", sortby);
        param.put("order", order);
        param.put("hasDbaEncode", hasDbaEncode);
        PageHelper.startPage(pageNum, pageSize);
        List<Map<String, Object>> resources = mapper.selectResources(param);
        // 分页结果
        return new PageInfo(resources);
    }

    public List<Map<String, Object>> queryColumns(Integer tableId) {
        return mapper.selectColumns(tableId);
    }

    public int insert(Project project) {
        mapper.insert(project);
        return project.getId();
    }

    public Project select(int id) {
        return mapper.selectByPrimaryKey(id);
    }

    public int update(Project project) {
        return mapper.updateByPrimaryKey(project);
    }

    public int delete(int id) {
        Project project = this.select(id);
        if (project != null) {
            logger.info("********* delete project start ,projectId:{},projectName;{} *********",
                    project.getId(), project.getProjectName());
            try {
                String path = StringUtils.joinWith("/", Constants.ROUTER_ROOT, project.getProjectName());
                if (zkService.isExists(path)) {
                    zkService.rmr(path);
                    logger.info("delete project znode:{}", path);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            long start = System.currentTimeMillis();
            encodeColumnsMapper.deleteByProjectId(id);
            long start1 = System.currentTimeMillis();
            logger.info("delete table t_project_topo_table_encode_output_columns success,cost time {}", start1 - start);

            metaVersionMapper.deleteByProjectId(id);
            long start2 = System.currentTimeMillis();
            logger.info("delete table t_project_topo_table_meta_version success,cost time {}", start2 - start1);

            encodeHintMapper.deleteByProjectId(id);
            long start3 = System.currentTimeMillis();
            logger.info("delete table t_project_encode_hint success,cost time {}", start3 - start2);

            resourceMapper.deleteByProjectId(id);
            long start4 = System.currentTimeMillis();
            logger.info("delete table t_project_resource success,cost time {}", start4 - start3);

            projectTopoTableMapper.deleteByProjectId(id);
            long start5 = System.currentTimeMillis();
            logger.info("delete table t_project_topo_table success,cost time {}", start5 - start4);

            mapper.deleteByPrimaryKey(id);
            long start6 = System.currentTimeMillis();
            logger.info("delete table t_project_topo,t_project_sink,t_project_sink,t_project_user success,cost time {}", start6 - start5);
            logger.info("******* delete project end ,projectId:{},projectName;{} cost time {} ******", start6 - start);
        }
        return 0;
    }

    public List<Map<String, Object>>  queryProjects(Map<String, Object> param) {
        return mapper.selectProjects(param);
    }

    public List<Map<String, Object>>  queryUserRelationProjects(Map<String, Object> param) {
        return mapper.selectUserRelationProjects(param);
    }

    public List<String> queryEncoderRules() throws Exception {
        List<String> rules = new ArrayList<>();
        if (zkService.isExists(Constants.ENCODER_PLUGINS)) {
            Properties props = zkService.getProperties(Constants.ENCODER_PLUGINS);
            Enumeration<String> names = (Enumeration<String>) props.propertyNames();
            while (names.hasMoreElements()) {
                String name = names.nextElement();
                CollectionUtils.addAll(rules, StringUtils.splitByWholeSeparator(props.getProperty(name), ","));
            }
        }
        return rules;
    }

	public String getPrincipal(int id) {
        return mapper.getPrincipal(id);
	}

	public List<Project> getMountedProjrct(Integer dsId, Integer tableId, Integer sinkId) {
        return mapper.getMountedProjrct(dsId, tableId, sinkId);
	}

    /**
     * 项目有效期检验:每天上午10点 0 0 10 * * ?
     */
    @Scheduled(cron = "0 0 10 * * ?")
    private void checkPeriodOfValidityTask()throws Exception {
        /*
        * 1.扫描项目：需要id,有效期
        * 2.有效期对比：
        *       1）发送邮件节点，一个月，15天，5天， 下线时通知
        *       2）邮件：
        *       2）颜色标识：一个月标记为橙色，下线后标记为红色
        *       3）到期后，项目下线
        */
        Properties properties = zkService.getProperties(
                Constants.KEEPER_ROOT+"/"+KeeperConstants.KEEPER_PROJECT_CONFIG);
        String reminderTimeStr = properties.getProperty(KeeperConstants.PROJECT_REMINDER_TIME);

        //倒排
        List<Integer> sortedReminderDays = Arrays.asList(reminderTimeStr.split(",")).stream().map(s -> Integer.parseInt(s.trim()))
                .sorted((n1,n2)->n2.compareTo(n1)).collect(Collectors.toList());
        logger.info("[project expire]项目有效期：提醒时间,{}",sortedReminderDays);


        List<Project> projects = mapper.selectAll();
        for(Project project : projects) {
            try {
                if(StringUtils.equals(project.getStatus(),ProjectStatus.INACTIVE.getStatus())){
                    continue;
                }

                Date expireDate = project.getProjectExpire();
                Date currentDate = new Date();
                long remainingTime = expireDate.getTime() - currentDate.getTime();

                //使用该对象进行更新projectRemainTime的值，如果需要
                Project updates = new Project();
                updates.setId(project.getId());

                /*剩余天数向下取整：例如：如果设置提前5天的时候提醒，那么在5~6天之间的时候的提醒，向下取整后就是设置的提醒天数5*/
                long remainDays=remainingTime/ProjectRemainTimeType.ONE_DAY;
                logger.info("[project expire]项目有效期：project:{},剩余时间:{}",project.getProjectName(),remainDays);

                //下线邮件提醒
                if(remainingTime <= 0){
                    //过期的当天下线提醒
                    if(remainingTime > ONE_DAY*(-1)) {
                        sendProjectExpireEmail(project);
                        updates.setStatus(ProjectStatus.INACTIVE.getStatus());
                        update(updates);
                    }
                }else{
                    //临到期邮件提醒
                    for(int daysBefore :sortedReminderDays){
                        //当天通知,如果检查的时候,剩余时间<（x=1）,即：正好x天或x.1~x.9天都是剩余x天
                        //if(remainingTime < daysBefore*ONE_DAY && remainingTime >(daysBefore-1)*ONE_DAY){
                        if(remainDays == daysBefore){
                            sendProjectExpireEmail(project);
                            break;
                        }
                    }

                }

               /* if (remainingTime > MONTH_DAY) {
                    continue;
                } else if (remainingTime > BEFORE_DAY) {
                *//* MONTH_DAY >= remainingTime > BEFORE_DAY,
                 * 没有精确判断remainingTime在30天当天，是为了兼容NULL值以及用户新添加一个有效期<30填的Project等情况
                 *//*

                    //提前一个月的时候，发送邮件，标记为MONTH：已经发送了MONTH的邮件，前端应该变化颜色
                    //如果已经标记为MONTH，不用重复标记
                    if (projectRemainTime != MONTH) {
                        sendProjectExpireEmail(project);
                        updates.setProjectRemainTime(MONTH.getTime());
                        update(updates);
                    }
                } else if (remainingTime > 0) {
                    //提前5天，发送邮件，标记为
                    if (projectRemainTime != BEFORE) {
                        sendProjectExpireEmail(project);
                        updates.setProjectRemainTime(BEFORE.getTime());
                        update(updates);
                    }
                } else {
                    //过期
                    if (projectRemainTime != EXPIRE) {
                        sendProjectExpireEmail(project);
                        updates.setProjectRemainTime(EXPIRE.getTime());
                        //下线
                        updates.setStatus(ProjectStatus.INACTIVE.getStatus());
                        update(updates);

                    }
                }*/
            }catch (Exception e){
                logger.info("[check period of validity exception] project:{}, exception:{}",project.getProjectName(),e);
                continue;
            }

        }

    }

    /**
     * 发送通知邮件，具体的发送时交给心跳
     */
    private void sendProjectExpireEmail(Project project) throws Exception{
        //发送的地址
        List<String> emails =projectUserMapper.selectProjectExpireEmails(project.getId());
        //消息载体
        ControlMessage msg = new ControlMessage(
                System.currentTimeMillis(),
                ControlType.KEEPER_PROJECT_EXPIRE.toString(),
                ProjectService.class.getName());
        long currentTime = System.currentTimeMillis();
        long remainingTime = project.getProjectExpire().getTime() - currentTime ;
        //剩余天数
        long remainDays;
        //项目下线时间，如果是下线告警需要
        String offlineTime = StringUtils.EMPTY;
        if(remainingTime <= 0){
            remainDays = 0;
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            offlineTime = df.format(currentTime);
            msg.addPayload("type", "项目下线");
        }else {
            remainDays=remainingTime/ProjectRemainTimeType.ONE_DAY;
            msg.addPayload("type", "项目临到期");
        }


        msg.addPayload("project_name", project.getProjectName());
        msg.addPayload("expire_time", project.getProjectExpire());
        msg.addPayload("remain_time", remainDays);
        msg.addPayload("emails", emails);
        msg.addPayload("offline_time", offlineTime);

        try{
            Properties configureProps = zkService.getProperties(KeeperConstants.KEEPER_CONFTEMPLATE_CONFIGURE);
            String topic = configureProps.getProperty(CONF_KEY_GLOBAL_EVENT_TOPIC);
            Properties props = zkService.getProperties(KeeperConstants.KEEPER_CTLMSG_PRODUCER_CONF);
            props.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

            Producer<String, String> producer = new KafkaProducer<String, String>(props);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, msg.getType(), msg.toJSONString());
            logger.info("[project expire]发送邮件消息:msg:{}",msg.toJSONString());
            Future<RecordMetadata> future = producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    logger.error("[project expire]Send global event error.{}", exception.getMessage());
                }
            });
            try {
                future.get(10000, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }


        }catch (Exception e){
            logger.info("[project expire]send email error: projectName:{}, emails:{}, Exception:{}",project.getProjectName(),emails,e);
            throw new RuntimeException(e);
        }

    }

    public PageInfo<Project> search(int pageNum, int pageSize, String sortby, String order) {
        Map<String, Object> map = new HashMap<>();
        map.put("sortby", sortby);
        map.put("order", order);
        PageHelper.startPage(pageNum, pageSize);
        return new PageInfo(mapper.search(map));
    }

    public List<ProjectTopoTable> getRunningTopoTables(Integer id) {
        return projectTopoTableMapper.selectRunningByProjectId(id);
    }

    public List<Map<String, Object>> getAllResourcesByQuery(String dsName, String schemaName, String tableName) {
        Map<String, Object> param = new HashMap<>();
        param.put("dsName", dsName);
        param.put("schemaName", schemaName);
        param.put("tableName", tableName);
        return mapper.selectResources(param);
    }
}
