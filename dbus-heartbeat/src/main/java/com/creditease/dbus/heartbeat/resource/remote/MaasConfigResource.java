package com.creditease.dbus.heartbeat.resource.remote;

import com.creditease.dbus.heartbeat.container.HeartBeatConfigContainer;
import com.creditease.dbus.heartbeat.vo.MaasVo;
import org.apache.commons.lang.StringUtils;

import java.io.ByteArrayInputStream;
import java.util.Properties;

/**
 * Created by dashencui on 2017/9/12.
 * 获取关于maas的所有配置信息
 */
public class MaasConfigResource extends ZkConfigResource<MaasVo> {
    public MaasConfigResource() {
        this(StringUtils.EMPTY);
    }

    protected MaasConfigResource(String name) {
        super(name);
    }

    @Override
    public MaasVo parse() {
        String configpath = StringUtils.EMPTY;
        String consumerpath = StringUtils.EMPTY;
        String producerpath = StringUtils.EMPTY;
        MaasVo maasvo = new MaasVo();
        try {
            configpath = HeartBeatConfigContainer.getInstance().getZkConf().getMaas_configPath();
            byte[] config_bytes = curator.getData().forPath(configpath);
            Properties config_props = new Properties();
            config_props.load(new ByteArrayInputStream(config_bytes));

            consumerpath = HeartBeatConfigContainer.getInstance().getZkConf().getMaas_consumerPath();
            byte[] consumer_bytes = curator.getData().forPath(consumerpath);
            Properties consumer_props = new Properties();
            consumer_props.load(new ByteArrayInputStream(consumer_bytes));

            producerpath = HeartBeatConfigContainer.getInstance().getZkConf().getMaas_producerPath();
            byte[] producer_bytes = curator.getData().forPath(producerpath);
            Properties producer_props = new Properties();
            producer_props.load(new ByteArrayInputStream(producer_bytes));


            if (config_bytes == null || config_bytes.length == 0) {
                throw new RuntimeException("[load-zk-maas-config] 加载zk-maas configpath: " + configpath + "配置信息不存在.");
            }
            maasvo.setConfigProp(config_props);
            maasvo.setConsumerProp(consumer_props);
            maasvo.setProducerProp(producer_props);

        } catch (Exception e) {
            throw new RuntimeException("[load-zk-config] 加载zk path: " + configpath + "配置信息出错.", e);
        }
        return  maasvo;
    }
}
