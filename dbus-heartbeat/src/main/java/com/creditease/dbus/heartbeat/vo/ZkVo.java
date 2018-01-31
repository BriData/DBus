
package com.creditease.dbus.heartbeat.vo;

import java.io.Serializable;

public class ZkVo implements Serializable {

    /**
     * serialVersionUID
     */
    private static final long serialVersionUID = 1319087951603432207L;

    private String zkStr;

    private Integer zkSessionTimeout;

    private Integer zkConnectionTimeout;

    private Integer zkRetryInterval;

    private String configPath;

    private String leaderPath;

    private String maas_configPath;

    private String maas_consumerPath;

    private String maas_producerPath;

    public String getZkStr() {
        return zkStr;
    }

    public void setZkStr(String zkStr) {
        this.zkStr = zkStr;
    }

    public Integer getZkSessionTimeout() {
        return zkSessionTimeout;
    }

    public void setZkSessionTimeout(Integer zkSessionTimeout) {
        this.zkSessionTimeout = zkSessionTimeout;
    }

    public Integer getZkConnectionTimeout() {
        return zkConnectionTimeout;
    }

    public void setZkConnectionTimeout(Integer zkConnectionTimeout) {
        this.zkConnectionTimeout = zkConnectionTimeout;
    }

    public Integer getZkRetryInterval() {
        return zkRetryInterval;
    }

    public void setZkRetryInterval(Integer zkRetryInterval) {
        this.zkRetryInterval = zkRetryInterval;
    }

    public String getConfigPath() {
        return configPath;
    }

    public String getLeaderPath(){return leaderPath;}

    public void setConfigPath(String configPath) {
        this.configPath = configPath;
    }

    public void setLeaderPath(String leaderPath){
        this.leaderPath = leaderPath;
    }

    public String getMaas_configPath(){
        return maas_configPath;
    }

    public  void setMaas_configPath(String maas_configPath){this.maas_configPath = maas_configPath;}

    public String getMaas_consumerPath(){
        return maas_consumerPath;
    }

    public void setMaas_consumerPath(String maas_consumerPath){ this.maas_consumerPath = maas_consumerPath;}

    public String getMaas_producerPath(){
        return maas_producerPath;
    }

    public void setMaas_producerPath(String maas_producerPath){ this.maas_producerPath = maas_producerPath;}

}
