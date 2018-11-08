package com.creditease.dbus.ogg.bean;

/**
 * User: 王少楠
 * Date: 2018-08-24
 * Desc:
 */
public class ConfigBean {
    private String oggBigHome;

    private String dsName;

    private String oggUser;

    private String oggPwd;

    private String oggUrl;

    private String kafkaProducerName;

    private String kafkaUrl;

    private String[] tables;

    private String[] appendTables;

    private String nlsLang;

    public String getNlsLang() {
        return nlsLang;
    }

    public void setNlsLang(String nlsLang) {
        this.nlsLang = nlsLang;
    }

    public String[] getTables() {
        return tables;
    }

    public void setTables(String[] tables) {
        this.tables = tables;
    }

    public String[] getAppendTables() {
        return appendTables;
    }

    public void setAppendTables(String[] appendTables) {
        this.appendTables = appendTables;
    }

    public String getOggBigHome() {
        return oggBigHome;
    }

    public void setOggBigHome(String oggBigHome) {
        this.oggBigHome = oggBigHome;
    }

    public String getDsName() {
        return dsName;
    }

    public void setDsName(String dsName) {
        this.dsName = dsName;
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

    public String getOggUrl() {
        return oggUrl;
    }

    public void setOggUrl(String oggUrl) {
        this.oggUrl = oggUrl;
    }

    public String getKafkaProducerName() {
        return kafkaProducerName;
    }

    public void setKafkaProducerName(String kafkaProducerName) {
        this.kafkaProducerName = kafkaProducerName;
    }

    public String getKafkaUrl() {
        return kafkaUrl;
    }

    public void setKafkaUrl(String kafkaUrl) {
        this.kafkaUrl = kafkaUrl;
    }
}
