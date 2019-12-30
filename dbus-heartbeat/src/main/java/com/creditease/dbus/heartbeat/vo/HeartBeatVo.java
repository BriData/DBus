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


package com.creditease.dbus.heartbeat.vo;

import java.io.Serializable;
import java.util.Map;

public class HeartBeatVo implements Serializable {

    /**
     * serialVersionUID
     */
    private static final long serialVersionUID = -4849126266520972270L;

    /**
     * heartbeat频率
     */
    private Long heartbeatInterval;

    private Long checkInterval;

    private Long checkFullPullInterval;

    private Long alarmTtl;

    private Integer maxAlarmCnt;

    private Long heartBeatTimeout;

    private Map<String, Map<String, String>> heartBeatTimeoutAdditional;

    private Long fullPullTimeout;

    private String leaderPath;

    private String controlPath;

    private String monitorPath;

    private String projectMonitorPath;

    private String monitorFullPullPath;

    private Long lifeInterval;

    private Long correcteValue;

    private Long fullPullCorrecteValue;

    private Long fullPullSliceMaxPending;

    private String excludeSchema;

    private Integer fullPullOldVersionCnt;

    private Integer deleteFullPullOldVersionInterval;

    private Integer checkPointPerHeartBeatCnt;

    private String adminSMSNo;

    private String adminUseSMS;

    private String adminEmail;

    private String adminUseEmail;

    private String schemaChangeEmail;

    private String schemaChangeUseEmail;

    private Integer queryTimeout;

    private Map<String, Map<String, String>> additionalNotify;

    private Long checkMasterSlaveDelayInterval;

    private Long checkMysqlBinlogInterval;

    private Long masterSlaveDelayTimeout;

    private String alarmSendEmail;
    private String alarmMailSMTPAddress;
    private String alarmMailSMTPPort;
    private String alarmMailUser;
    private String alarmMailPass;

    public Long getHeartbeatInterval() {
        return heartbeatInterval;
    }

    public void setHeartbeatInterval(Long heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
    }

    public Long getCheckInterval() {
        return checkInterval;
    }

    public void setCheckInterval(Long checkInterval) {
        this.checkInterval = checkInterval;
    }

    public Long getAlarmTtl() {
        return alarmTtl;
    }

    public void setAlarmTtl(Long alarmTtl) {
        this.alarmTtl = alarmTtl;
    }

    public Integer getMaxAlarmCnt() {
        return maxAlarmCnt;
    }

    public void setMaxAlarmCnt(Integer maxAlarmCnt) {
        this.maxAlarmCnt = maxAlarmCnt;
    }

    public String getLeaderPath() {
        return leaderPath;
    }

    public void setLeaderPath(String leaderPath) {
        this.leaderPath = leaderPath;
    }

    public String getControlPath() {
        return controlPath;
    }

    public void setControlPath(String controlPath) {
        this.controlPath = controlPath;
    }

    public String getMonitorPath() {
        return monitorPath;
    }

    public String getProjectMonitorPath() {
        return projectMonitorPath;
    }

    public void setProjectMonitorPath(String projectMonitorPath) {
        this.projectMonitorPath = projectMonitorPath;
    }

    public void setMonitorPath(String monitorPath) {
        this.monitorPath = monitorPath;
    }

    public Long getHeartBeatTimeout() {
        return heartBeatTimeout;
    }

    public void setHeartBeatTimeout(Long heartBeatTimeout) {
        this.heartBeatTimeout = heartBeatTimeout;
    }

    public Map<String, Map<String, String>> getHeartBeatTimeoutAdditional() {
        return heartBeatTimeoutAdditional;
    }

    public void setHeartBeatTimeoutAdditional(Map<String, Map<String, String>> heartBeatTimeoutAdditional) {
        this.heartBeatTimeoutAdditional = heartBeatTimeoutAdditional;
    }

    public Long getLifeInterval() {
        return lifeInterval;
    }

    public void setLifeInterval(Long lifeInterval) {
        this.lifeInterval = lifeInterval;
    }

    public Long getCorrecteValue() {
        return correcteValue;
    }

    public void setCorrecteValue(Long correcteValue) {
        this.correcteValue = correcteValue;
    }

    public String getMonitorFullPullPath() {
        return monitorFullPullPath;
    }

    public void setMonitorFullPullPath(String monitorFullPullPath) {
        this.monitorFullPullPath = monitorFullPullPath;
    }

    public Long getCheckFullPullInterval() {
        return checkFullPullInterval;
    }

    public void setCheckFullPullInterval(Long checkFullPullInterval) {
        this.checkFullPullInterval = checkFullPullInterval;
    }

    public Long getFullPullTimeout() {
        return fullPullTimeout;
    }

    public void setFullPullTimeout(Long fullPullTimeout) {
        this.fullPullTimeout = fullPullTimeout;
    }

    public Long getFullPullCorrecteValue() {
        return fullPullCorrecteValue;
    }

    public void setFullPullCorrecteValue(Long fullPullCorrecteValue) {
        this.fullPullCorrecteValue = fullPullCorrecteValue;
    }

    public Long getFullPullSliceMaxPending() {
        return fullPullSliceMaxPending;
    }

    public void setFullPullSliceMaxPending(Long fullPullSliceMaxPending) {
        this.fullPullSliceMaxPending = fullPullSliceMaxPending;
    }

    public String getExcludeSchema() {
        return excludeSchema;
    }

    public void setExcludeSchema(String excludeSchema) {
        this.excludeSchema = excludeSchema;
    }

    public Integer getFullPullOldVersionCnt() {
        return fullPullOldVersionCnt;
    }

    public void setFullPullOldVersionCnt(Integer fullPullOldVersionCnt) {
        this.fullPullOldVersionCnt = fullPullOldVersionCnt;
    }

    public Integer getDeleteFullPullOldVersionInterval() {
        return deleteFullPullOldVersionInterval;
    }

    public void setDeleteFullPullOldVersionInterval(
            Integer deleteFullPullOldVersionInterval) {
        this.deleteFullPullOldVersionInterval = deleteFullPullOldVersionInterval;
    }

    public Integer getCheckPointPerHeartBeatCnt() {
        return checkPointPerHeartBeatCnt;
    }

    public void setCheckPointPerHeartBeatCnt(Integer checkPointPerHeartBeatCnt) {
        this.checkPointPerHeartBeatCnt = checkPointPerHeartBeatCnt;
    }

    public String getAdminSMSNo() {
        return adminSMSNo;
    }

    public void setAdminSMSNo(String adminSMSNo) {
        this.adminSMSNo = adminSMSNo;
    }

    public String getAdminUseSMS() {
        return adminUseSMS;
    }

    public void setAdminUseSMS(String adminUseSMS) {
        this.adminUseSMS = adminUseSMS;
    }

    public String getAdminEmail() {
        return adminEmail;
    }

    public void setAdminEmail(String adminEmail) {
        this.adminEmail = adminEmail;
    }

    public String getAdminUseEmail() {
        return adminUseEmail;
    }

    public void setAdminUseEmail(String adminUseEmail) {
        this.adminUseEmail = adminUseEmail;
    }

    public Map<String, Map<String, String>> getAdditionalNotify() {
        return additionalNotify;
    }

    public void setAdditionalNotify(
            Map<String, Map<String, String>> additionalNotify) {
        this.additionalNotify = additionalNotify;
    }

    public String getSchemaChangeEmail() {
        return schemaChangeEmail;
    }

    public void setSchemaChangeEmail(String schemaChangeEmail) {
        this.schemaChangeEmail = schemaChangeEmail;
    }

    public String getSchemaChangeUseEmail() {
        return schemaChangeUseEmail;
    }

    public void setSchemaChangeUseEmail(String schemaChangeUseEmail) {
        this.schemaChangeUseEmail = schemaChangeUseEmail;
    }

    public Integer getQueryTimeout() {
        return queryTimeout;
    }

    public void setQueryTimeout(Integer queryTimeout) {
        this.queryTimeout = queryTimeout;
    }

    public Long getCheckMasterSlaveDelayInterval() {
        return checkMasterSlaveDelayInterval;
    }

    public void setCheckMasterSlaveDelayInterval(Long checkMasterSlaveDelayInterval) {
        this.checkMasterSlaveDelayInterval = checkMasterSlaveDelayInterval;
    }

    public Long getMasterSlaveDelayTimeout() {
        return masterSlaveDelayTimeout;
    }

    public void setMasterSlaveDelayTimeout(Long masterSlaveDelayTimeout) {
        this.masterSlaveDelayTimeout = masterSlaveDelayTimeout;
    }

    public String getAlarmSendEmail() {
        return alarmSendEmail;
    }

    public void setAlarmSendEmail(String alarmSendEmail) {
        this.alarmSendEmail = alarmSendEmail;
    }

    public String getAlarmMailSMTPAddress() {
        return alarmMailSMTPAddress;
    }

    public void setAlarmMailSMTPAddress(String alarmMailSMTPAddress) {
        this.alarmMailSMTPAddress = alarmMailSMTPAddress;
    }

    public String getAlarmMailSMTPPort() {
        return alarmMailSMTPPort;
    }

    public void setAlarmMailSMTPPort(String alarmMailSMTPPort) {
        this.alarmMailSMTPPort = alarmMailSMTPPort;
    }

    public String getAlarmMailUser() {
        return alarmMailUser;
    }

    public void setAlarmMailUser(String alarmMailUser) {
        this.alarmMailUser = alarmMailUser;
    }

    public String getAlarmMailPass() {
        return alarmMailPass;
    }

    public void setAlarmMailPass(String alarmMailPass) {
        this.alarmMailPass = alarmMailPass;
    }

    public Long getCheckMysqlBinlogInterval() {
        return checkMysqlBinlogInterval;
    }

    public void setCheckMysqlBinlogInterval(Long checkMysqlBinlogInterval) {
        this.checkMysqlBinlogInterval = checkMysqlBinlogInterval;
    }
}
