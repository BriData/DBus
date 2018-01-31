package com.creditease.dbus.log.processor.helper;

import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.ControlMessage;
import com.creditease.dbus.commons.CtlMessageResult;
import com.creditease.dbus.commons.CtlMessageResultSender;
import com.creditease.dbus.commons.IZkService;
import com.creditease.dbus.commons.ZkService;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.creditease.dbus.commons.Constants.LOG_PROCESSOR;


public class ZkHelper {

    private static Logger logger = LoggerFactory.getLogger(ZkHelper.class);

    private String zkStr = null;
    private String topologyId = null;
    private IZkService zkService = null;

    public ZkHelper(String zkStr, String topologyId) {
        this.zkStr = zkStr;
        this.topologyId = topologyId;
        try {
            zkService = new ZkService(zkStr);
        } catch (Exception e) {
            logger.error("ZkHelper init error.", e);
            throw new RuntimeException("ZkHelper init error", e);
        }
    }

    public Properties loadLogProcessorConf() {
        String path = Constants.TOPOLOGY_ROOT + "/" + topologyId + "-" + LOG_PROCESSOR + "/config.properties";
        try {
            return zkService.getProperties(path);
        } catch (Exception e) {
            logger.error("load config.properties error.", e);
            throw new RuntimeException("load config.properties error", e);
        }
    }

    public boolean saveLogProcessorConf(Properties props) {
        String path = Constants.TOPOLOGY_ROOT + "/" + topologyId + "-" + LOG_PROCESSOR + "/config.properties";
        try {
            logger.info("保存log processor conf 成功！");
            return zkService.setProperties(path, props);
        } catch (Exception e) {
            logger.error("save config.properties error.", e);
            throw new RuntimeException("save config.properties error", e);
        }
    }

    public Properties loadKafkaConsumerConf() {
        String path = Constants.TOPOLOGY_ROOT + "/" + topologyId + "-" + LOG_PROCESSOR + "/consumer.properties";
        try {
            return zkService.getProperties(path);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("load consumer.properties error.", e);
            throw new RuntimeException("load consumer.properties error", e);
        }
    }

    public Properties loadKafkaProducerConf() {
        String path = Constants.TOPOLOGY_ROOT + "/" + topologyId + "-" + LOG_PROCESSOR + "/producer.properties";
        try {
            return zkService.getProperties(path);
        } catch (Exception e) {
            logger.error("load producer.properties error.", e);
            throw new RuntimeException("load producer.properties error", e);
        }
    }

    public Properties loadMySqlConf() {
        String path = Constants.COMMON_ROOT + "/"  + Constants.MYSQL_PROPERTIES;
        try {
            return zkService.getProperties(path);
        } catch (Exception e) {
            logger.error("load mysql.properties error.", e);
            throw new RuntimeException("load mysql.properties error", e);
        }
    }

    public void saveReloadStatus(String json, String title, boolean prepare) {
        if (json != null) {
            String msg = title + " reload successful!";
            ControlMessage message = ControlMessage.parse(json);
            CtlMessageResult result = new CtlMessageResult(title, msg);
            result.setOriginalMessage(message);
            CtlMessageResultSender sender = new CtlMessageResultSender(message.getType(), zkStr);
            sender.send(title, result, prepare, false);
        }
    }

    public ZkService getZkservice() {
        return (ZkService) zkService;
    }

    public void close() {
        try {
            zkService.close();
        } catch (Exception e) {
            logger.error("close error.", e);
        }
    }
}
