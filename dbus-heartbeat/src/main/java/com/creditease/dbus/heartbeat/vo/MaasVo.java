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
import java.util.Properties;

/**
 * Created by dashencui on 2017/9/12.
 */
public class MaasVo implements Serializable {

    /**
     * serialVersionUID
     */
    private static final long serialVersionUID = 1L;

    /**
     * maas配置信息
     */
    private Properties configProp;
    private Properties consumerProp;
    private Properties producerProp;

    public Properties getConfigProp() {
        return configProp;
    }

    public void setConfigProp(Properties configProp) {
        this.configProp = configProp;
    }

    public Properties getConsumerProp() {
        return consumerProp;
    }

    public void setConsumerProp(Properties consumerProp) {
        this.consumerProp = consumerProp;
    }

    public Properties getProducerProp() {
        return producerProp;
    }

    public void setProducerProp(Properties producerProp) {
        this.producerProp = producerProp;
    }
}
