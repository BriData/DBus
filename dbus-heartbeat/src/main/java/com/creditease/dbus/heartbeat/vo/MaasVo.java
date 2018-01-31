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

    /** maas配置信息  */
    private Properties configProp;
    private Properties consumerProp;
    private Properties producerProp;

    public Properties getConfigProp(){
        return configProp;
    }

    public void setConfigProp(Properties configProp){
        this.configProp = configProp;
    }

    public Properties getConsumerProp(){
        return consumerProp;
    }

    public void setConsumerProp(Properties consumerProp){
        this.consumerProp = consumerProp;
    }

    public Properties getProducerProp(){
        return producerProp;
    }

    public void setProducerProp(Properties producerProp){
        this.producerProp = producerProp;
    }
}
