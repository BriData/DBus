package com.creditease.dbus.canal.deploy.bean;
/**
 * User: 王少楠
 * Date: 2018-08-02
 * Desc:
 */
public class DeployPropsBean {
    /** 数据源名称 */
    private String dsName;
    /** zk地址 */
    private String zkPath;
    /** 备库地址 */
    private String slavePath;
    /** canal用户名 */
    private String canalUser ;
    /** canal 用户密码 */
    private String canalPwd;

    public String getDsName() {
        return dsName;
    }

    public void setDsName(String dsName) {
        this.dsName = dsName;
    }

    public String getZkPath() {
        return zkPath;
    }

    public void setZkPath(String zkPath) {
        this.zkPath = zkPath;
    }

    public String getSlavePath() {
        return slavePath;
    }

    public void setSlavePath(String slavePath) {
        this.slavePath = slavePath;
    }

    public String getCanalUser() {
        return canalUser;
    }

    public void setCanalUser(String canalUser) {
        this.canalUser = canalUser;
    }

    public String getCanalPwd() {
        return canalPwd;
    }

    public void setCanalPwd(String canalPwd) {
        this.canalPwd = canalPwd;
    }
}
