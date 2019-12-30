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


package com.creditease.dbus.allinone.auto.check.resource.local;

import com.creditease.dbus.allinone.auto.check.bean.AutoCheckConfigBean;

public class AutoCheckFileConfigResource extends FileConfigResource<AutoCheckConfigBean> {

    public AutoCheckFileConfigResource(String name) {
        super(name);
    }

    @Override
    public AutoCheckConfigBean parse() {
        AutoCheckConfigBean conf = new AutoCheckConfigBean();
        try {
            conf.setDbDbusmgrHost(prop.getProperty("db.dbusmgr.host"));
            conf.setDbDbusmgrPort(Integer.valueOf(prop.getProperty("db.dbusmgr.port")));
            conf.setDbDbusmgrIsPrint(Boolean.valueOf(prop.getProperty("db.dbusmgr.isPrint")));
            conf.setDbDbusmgrSchema(prop.getProperty("db.dbusmgr.schema"));
            conf.setDbDbusmgrPassword(prop.getProperty("db.dbusmgr.password"));
            conf.setDbDbusmgrTestSql(prop.getProperty("db.dbusmgr.test.sql"));

            conf.setDbDbusHost(prop.getProperty("db.dbus.host"));
            conf.setDbDbusPort(Integer.valueOf(prop.getProperty("db.dbus.port")));
            conf.setDbDbusIsPrint(Boolean.valueOf(prop.getProperty("db.dbus.isPrint")));
            conf.setDbDbusSchema(prop.getProperty("db.dbus.schema"));
            conf.setDbDbusPassword(prop.getProperty("db.dbus.password"));
            conf.setDbDbusTestSql(prop.getProperty("db.dbus.test.sql"));

            conf.setDbCanalHost(prop.getProperty("db.canal.host"));
            conf.setDbCanalPort(Integer.valueOf(prop.getProperty("db.canal.port")));
            conf.setDbCanalIsPrint(Boolean.valueOf(prop.getProperty("db.canal.isPrint")));
            conf.setDbCanalSchema(prop.getProperty("db.canal.schema"));
            conf.setDbCanalPassword(prop.getProperty("db.canal.password"));
            conf.setDbCanalTestSql(prop.getProperty("db.canal.test.sql"));

            conf.setDbTestSchemaHost(prop.getProperty("db.test.host"));
            conf.setDbTestSchemaPort(Integer.valueOf(prop.getProperty("db.test.port")));
            conf.setDbTestSchemaIsPrint(Boolean.valueOf(prop.getProperty("db.test.isPrint")));
            conf.setDbTestSchemaSchema(prop.getProperty("db.test.schema"));
            conf.setDbTestSchemaPassword(prop.getProperty("db.test.password"));
            conf.setDbTestSchemaTestSql(prop.getProperty("db.test.test.sql"));

            conf.setStormUIApi(prop.getProperty("storm.ui.api"));
            conf.setZkHost(prop.getProperty("zk.host"));
            conf.setCanalZkNode(prop.getProperty("canal.zk.node"));
            conf.setKafkaBootstrapServers(prop.getProperty("kafka.bootstrap.servers"));

        } catch (Exception e) {
            throw new RuntimeException("parse config resource " + name + " error!");
        }
        return conf;
    }

}
