package com.creditease.dbus.router.spout;

import java.util.Map;

import com.creditease.dbus.router.base.DBusRouterBase;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseRichSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseSpout extends BaseRichSpout {

    private static Logger logger = LoggerFactory.getLogger(BaseSpout.class);

    protected TopologyContext context = null;
    protected BaseSpoutInner inner = null;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.context = context;
        try {
            inner = new BaseSpoutInner(conf);
            inner.init();
            postOpen();
        } catch (Exception e) {
            logger.error("base spout open error", e);
        }
    }

    public TopologyContext getContext() {
        return context;
    }

    public abstract void postOpen() throws Exception;

    public abstract void initConsumer() throws Exception;

    protected class BaseSpoutInner extends DBusRouterBase {
        public BaseSpoutInner(Map conf) {
            super(conf);
        }
    }

}
