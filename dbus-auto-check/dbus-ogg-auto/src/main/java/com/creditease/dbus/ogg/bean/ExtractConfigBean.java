package com.creditease.dbus.ogg.bean;

/**
 * User: 王少楠
 * Date: 2018-08-28
 * Desc: OGG extract进程需要的配置
 */
public class ExtractConfigBean {

    private String[] appendTables;

    private String oggUser;

    private String oggPwd;

    private String oggHome;

    private String extrName;

    private String rmHost;

    private String mgrPort;

    private String extractFile;

    private String[] tables;

    private String nlsLang;

    public String getNlsLang() {
        return nlsLang;
    }

    public void setNlsLang(String nlsLang) {
        this.nlsLang = nlsLang;
    }

    public String[] getAppendTables() {
        return appendTables;
    }

    public void setAppendTables(String[] appendTables) {
        this.appendTables = appendTables;
    }

    public String getOggUser() {
        return oggUser;
    }

    public void setOggUser(String oggUser) {
        this.oggUser = oggUser;
    }

    public String getOggPwd() {
        return oggPwd;
    }

    public void setOggPwd(String oggPwd) {
        this.oggPwd = oggPwd;
    }

    public String getOggHome() {
        return oggHome;
    }

    public void setOggHome(String oggHome) {
        this.oggHome = oggHome;
    }

    public String getExtrName() {
        return extrName;
    }

    public void setExtrName(String extrName) {
        this.extrName = extrName;
    }

    public String getRmHost() {
        return rmHost;
    }

    public void setRmHost(String rmHost) {
        this.rmHost = rmHost;
    }

    public String getMgrPort() {
        return mgrPort;
    }

    public void setMgrPort(String mgrPort) {
        this.mgrPort = mgrPort;
    }

    public String getExtractFile() {
        return extractFile;
    }

    public void setExtractFile(String extractFile) {
        this.extractFile = extractFile;
    }

    public String[] getTables() {
        return tables;
    }

    public void setTables(String[] tables) {
        this.tables = tables;
    }
}
