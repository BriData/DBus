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


package com.creditease.dbus.allinone.auto.check.bean;

/**
 * Created by Administrator on 2018/8/1.
 */
public class AutoCheckConfigBean {

    private String dbDbusmgrHost;
    private int dbDbusmgrPort;
    private boolean dbDbusmgrIsPrint;
    private String dbDbusmgrSchema;
    private String dbDbusmgrPassword;
    private String dbDbusmgrTestSql;

    private String dbDbusHost;
    private int dbDbusPort;
    private boolean dbDbusIsPrint;
    private String dbDbusSchema;
    private String dbDbusPassword;
    private String dbDbusTestSql;

    private String dbCanalHost;
    private int dbCanalPort;
    private boolean dbCanalIsPrint;
    private String dbCanalSchema;
    private String dbCanalPassword;
    private String dbCanalTestSql;

    private String dbTestSchemaHost;
    private int dbTestSchemaPort;
    private boolean dbTestSchemaIsPrint;
    private String dbTestSchemaSchema;
    private String dbTestSchemaPassword;
    private String dbTestSchemaTestSql;

    private String stormUIApi;

    private String zkHost;
    private String canalZkNode;
    private String kafkaBootstrapServers;

    public String getDbDbusmgrHost() {
        return dbDbusmgrHost;
    }

    public void setDbDbusmgrHost(String dbDbusmgrHost) {
        this.dbDbusmgrHost = dbDbusmgrHost;
    }

    public int getDbDbusmgrPort() {
        return dbDbusmgrPort;
    }

    public void setDbDbusmgrPort(int dbDbusmgrPort) {
        this.dbDbusmgrPort = dbDbusmgrPort;
    }

    public boolean isDbDbusmgrIsPrint() {
        return dbDbusmgrIsPrint;
    }

    public void setDbDbusmgrIsPrint(boolean dbDbusmgrIsPrint) {
        this.dbDbusmgrIsPrint = dbDbusmgrIsPrint;
    }

    public String getDbDbusmgrSchema() {
        return dbDbusmgrSchema;
    }

    public void setDbDbusmgrSchema(String dbDbusmgrSchema) {
        this.dbDbusmgrSchema = dbDbusmgrSchema;
    }

    public String getDbDbusmgrPassword() {
        return dbDbusmgrPassword;
    }

    public void setDbDbusmgrPassword(String dbDbusmgrPassword) {
        this.dbDbusmgrPassword = dbDbusmgrPassword;
    }

    public String getDbDbusmgrTestSql() {
        return dbDbusmgrTestSql;
    }

    public void setDbDbusmgrTestSql(String dbDbusmgrTestSql) {
        this.dbDbusmgrTestSql = dbDbusmgrTestSql;
    }

    public String getDbDbusHost() {
        return dbDbusHost;
    }

    public void setDbDbusHost(String dbDbusHost) {
        this.dbDbusHost = dbDbusHost;
    }

    public int getDbDbusPort() {
        return dbDbusPort;
    }

    public void setDbDbusPort(int dbDbusPort) {
        this.dbDbusPort = dbDbusPort;
    }

    public boolean isDbDbusIsPrint() {
        return dbDbusIsPrint;
    }

    public void setDbDbusIsPrint(boolean dbDbusIsPrint) {
        this.dbDbusIsPrint = dbDbusIsPrint;
    }

    public String getDbDbusSchema() {
        return dbDbusSchema;
    }

    public void setDbDbusSchema(String dbDbusSchema) {
        this.dbDbusSchema = dbDbusSchema;
    }

    public String getDbDbusPassword() {
        return dbDbusPassword;
    }

    public void setDbDbusPassword(String dbDbusPassword) {
        this.dbDbusPassword = dbDbusPassword;
    }

    public String getDbDbusTestSql() {
        return dbDbusTestSql;
    }

    public void setDbDbusTestSql(String dbDbusTestSql) {
        this.dbDbusTestSql = dbDbusTestSql;
    }

    public String getDbCanalHost() {
        return dbCanalHost;
    }

    public void setDbCanalHost(String dbCanalHost) {
        this.dbCanalHost = dbCanalHost;
    }

    public int getDbCanalPort() {
        return dbCanalPort;
    }

    public void setDbCanalPort(int dbCanalPort) {
        this.dbCanalPort = dbCanalPort;
    }

    public boolean isDbCanalIsPrint() {
        return dbCanalIsPrint;
    }

    public void setDbCanalIsPrint(boolean dbCanalIsPrint) {
        this.dbCanalIsPrint = dbCanalIsPrint;
    }

    public String getDbCanalSchema() {
        return dbCanalSchema;
    }

    public void setDbCanalSchema(String dbCanalSchema) {
        this.dbCanalSchema = dbCanalSchema;
    }

    public String getDbCanalPassword() {
        return dbCanalPassword;
    }

    public void setDbCanalPassword(String dbCanalPassword) {
        this.dbCanalPassword = dbCanalPassword;
    }

    public String getDbCanalTestSql() {
        return dbCanalTestSql;
    }

    public void setDbCanalTestSql(String dbCanalTestSql) {
        this.dbCanalTestSql = dbCanalTestSql;
    }

    public String getDbTestSchemaHost() {
        return dbTestSchemaHost;
    }

    public void setDbTestSchemaHost(String dbTestSchemaHost) {
        this.dbTestSchemaHost = dbTestSchemaHost;
    }

    public int getDbTestSchemaPort() {
        return dbTestSchemaPort;
    }

    public void setDbTestSchemaPort(int dbTestSchemaPort) {
        this.dbTestSchemaPort = dbTestSchemaPort;
    }

    public boolean isDbTestSchemaIsPrint() {
        return dbTestSchemaIsPrint;
    }

    public void setDbTestSchemaIsPrint(boolean dbTestSchemaIsPrint) {
        this.dbTestSchemaIsPrint = dbTestSchemaIsPrint;
    }

    public String getDbTestSchemaSchema() {
        return dbTestSchemaSchema;
    }

    public void setDbTestSchemaSchema(String dbTestSchemaSchema) {
        this.dbTestSchemaSchema = dbTestSchemaSchema;
    }

    public String getDbTestSchemaPassword() {
        return dbTestSchemaPassword;
    }

    public void setDbTestSchemaPassword(String dbTestSchemaPassword) {
        this.dbTestSchemaPassword = dbTestSchemaPassword;
    }

    public String getDbTestSchemaTestSql() {
        return dbTestSchemaTestSql;
    }

    public void setDbTestSchemaTestSql(String dbTestSchemaTestSql) {
        this.dbTestSchemaTestSql = dbTestSchemaTestSql;
    }

    public String getStormUIApi() {
        return stormUIApi;
    }

    public void setStormUIApi(String stormUIApi) {
        this.stormUIApi = stormUIApi;
    }

    public String getZkHost() {
        return zkHost;
    }

    public void setZkHost(String zkHost) {
        this.zkHost = zkHost;
    }

    public String getCanalZkNode() {
        return canalZkNode;
    }

    public void setCanalZkNode(String canalZkNode) {
        this.canalZkNode = canalZkNode;
    }

    public String getKafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }

    public void setKafkaBootstrapServers(String kafkaBootstrapServers) {
        this.kafkaBootstrapServers = kafkaBootstrapServers;
    }
}
