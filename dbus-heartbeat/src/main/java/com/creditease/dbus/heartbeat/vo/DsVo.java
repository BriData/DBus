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

public class DsVo extends JdbcVo {

    /**
     * serialVersionUID
     */
    private static final long serialVersionUID = -3754507199183909433L;

    /**
     * 数据源分库
     */
    private String dsPartition;

    /**
     * slave url
     */
    private String slvaeUrl;

    public String getDsPartition() {
        return dsPartition;
    }

    public void setDsPartition(String dsPartition) {
        this.dsPartition = dsPartition;
    }

    public String getSlvaeUrl() {
        return slvaeUrl;
    }

    public void setSlvaeUrl(String slvaeUrl) {
        this.slvaeUrl = slvaeUrl;
    }
}
