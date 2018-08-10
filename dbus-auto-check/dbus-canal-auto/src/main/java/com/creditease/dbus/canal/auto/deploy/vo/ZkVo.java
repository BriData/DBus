package com.creditease.dbus.canal.auto.deploy.vo;

import java.io.Serializable;

/**
 * User: 王少楠
 * Date: 2018-08-06
 * Desc:
 */
public class ZkVo implements Serializable {

    /**
     * serialVersionUID
     */
    private static final long serialVersionUID = 1319087951603432207L;

    private String zkStr;

    private Integer zkSessionTimeout;

    private Integer zkConnectionTimeout;

    private Integer zkRetryInterval;


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

    public static ZkVo loadZk(){
        //Properties zkProperties = ;//FileUtils.loadFileProperties(SystemUtils.USER_DIR.replaceAll("\\\\", "/") + "/conf/" + "zk.properties");
            /*zk.session.timeout=20000
             *zk.connection.timeout=25000
             * zk.retry.interval=30
             */
        ZkVo zk = new ZkVo();
        zk.setZkSessionTimeout(20000);
        zk.setZkConnectionTimeout(25000);
        zk.setZkRetryInterval(30);
        return zk;
    }

}

