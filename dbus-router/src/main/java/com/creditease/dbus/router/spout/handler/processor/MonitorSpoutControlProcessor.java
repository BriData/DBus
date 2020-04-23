package com.creditease.dbus.router.spout.handler.processor;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.commons.ControlType;
import com.creditease.dbus.router.bean.Sink;
import com.creditease.dbus.router.spout.aware.ContextAware;
import com.creditease.dbus.router.spout.context.Context;
import com.creditease.dbus.router.spout.context.ProcessorContext;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MonitorSpoutControlProcessor extends AbstractProcessor implements ContextAware {

    private static Logger logger = LoggerFactory.getLogger(MonitorSpoutControlProcessor.class);
    private ProcessorContext context = null;

    @Override
    public void setContext(Context context) {
        this.context = (ProcessorContext) context;
    }

    @Override
    public boolean isBelong(String str) {
        return isCtrlTopic(str);
    }

    @Override
    public Object process(Object obj, Supplier... suppliers) {
        Object ret = new Object();
        ConsumerRecord<String, byte[]> record = (ConsumerRecord) obj;
        logger.info("topic:{}, key:{}, offset:{}", record.topic(), record.key(), record.offset());
        if (!isBelong(record.key())) return ret;
        try {
            processCtrlMsg(record);
        } catch (Exception e) {
            logger.error("monitor control processor error", e);
        }
        return ret;
    }

    private String obtainCtrlTopic() {
        return StringUtils.joinWith("_", context.getInner().topologyId, "ctrl");
    }

    private boolean isCtrlTopic(String topic) {
        return StringUtils.equals(topic, obtainCtrlTopic());
    }

    private boolean processCtrlMsg(ConsumerRecord<String, byte[]> record) throws Exception {
        boolean ret = true;
        String strCtrl = new String(record.value(), "UTF-8");
        logger.info("monitor spout process ctrl msg. cmd:{}", strCtrl);
        JSONObject jsonCtrl = JSONObject.parseObject(strCtrl);
        ControlType cmd = ControlType.getCommand(jsonCtrl.getString("type"));
        switch (cmd) {
            case ROUTER_RELOAD:
                reload(strCtrl);
                break;
            case ROUTER_TOPOLOGY_TABLE_START:
                ret = startTopologyTable(jsonCtrl);
                break;
            case ROUTER_TOPOLOGY_TABLE_STOP:
                ret = stopTopologyTable(jsonCtrl);
                break;
            default:
                break;
        }
        return ret;
    }

    private void reload(String ctrl) throws Exception {
        context.getInner().close(true);
        context.getInner().init();
        context.getSpout().postOpen();
        context.getInner().zkHelper.saveReloadStatus(ctrl, "DBusRouterMonitorSpout-" + context.getSpout().getContext().getThisTaskId(), true);
        logger.info("monitor spout reload completed.");
    }

    private boolean startTopologyTable(JSONObject ctrl) throws Exception {
        boolean ret = false;
        JSONObject payload = ctrl.getJSONObject("payload");
        Integer projectTopologyTableId = payload.getInteger("projectTopoTableId");
        List<Sink> sinks = context.getInner().dbHelper.loadSinks(context.getInner().topologyId, projectTopologyTableId);
        if (sinks != null && sinks.size() > 0) {
            boolean isUsing = context.getInner().dbHelper.isUsingTopic(projectTopologyTableId);
            if (isUsing) {
                logger.info("table:{} of out put topic is using, don't need assign.", projectTopologyTableId);
            } else {
                ret = true;
                Optional.ofNullable(context.getSinks()).orElseGet(() -> {
                    List<Sink> list = new ArrayList<>();
                    context.setSinks(list);
                    return list;
                }).addAll(sinks);
                context.getSpout().initConsumer();
            }
        }
        logger.info("monitor spout start topology table:{} completed.", projectTopologyTableId);
        return ret;
    }

    private boolean stopTopologyTable(JSONObject ctrl) throws Exception {
        boolean ret = false;
        JSONObject payload = ctrl.getJSONObject("payload");
        String dsName = payload.getString("dsName");
        String schemaName = payload.getString("schemaName");
        String tableName = payload.getString("tableName");
        Integer projectTopologyTableId = payload.getInteger("projectTopoTableId");
        String namespace = StringUtils.joinWith(".", dsName, schemaName, tableName);

        List<Sink> sinks = context.getSinks();
        if (sinks != null) {
            Sink delSink = null;
            for (Sink sink : sinks) {
                String wkNs = StringUtils.joinWith(".", sink.getDsName(), sink.getSchemaName(), sink.getTableName());
                if (StringUtils.equals(wkNs, namespace)) {
                    delSink = sink;
                    break;
                }
            }
            if (delSink != null) {
                sinks.remove(delSink);
                boolean isUsing = context.getInner().dbHelper.isUsingTopic(projectTopologyTableId);
                if (isUsing) {
                    logger.info("table:{} of out put topic is using, don't need remove.", projectTopologyTableId);
                } else {
                    context.getSpout().initConsumer();
                    ret = true;
                }
            } else {
                logger.error("don't find name space:{} target sink, stop table fail.", namespace);
            }
        }
        logger.info("monitor spout stop topology table:{} completed.", projectTopologyTableId);
        return ret;
    }

}
