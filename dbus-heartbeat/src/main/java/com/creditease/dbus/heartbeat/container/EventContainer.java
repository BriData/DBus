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


package com.creditease.dbus.heartbeat.container;

import com.creditease.dbus.heartbeat.event.IEvent;
import com.creditease.dbus.heartbeat.log.LoggerFactory;
import com.creditease.dbus.heartbeat.type.DelayItem;
import com.creditease.dbus.heartbeat.util.JsonUtil;
import com.creditease.dbus.heartbeat.vo.MonitorNodeVo;
import com.creditease.dbus.heartbeat.vo.PacketVo;
import com.creditease.dbus.heartbeat.vo.TargetTopicVo;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.slf4j.Logger;

import java.nio.charset.Charset;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class EventContainer {
    protected Logger LOG = LoggerFactory.getLogger();

    private static EventContainer container;

    private ConcurrentHashMap<IEvent, Thread> cmp = new ConcurrentHashMap<IEvent, Thread>();

    private ConcurrentHashMap<String, String> skipSchema = new ConcurrentHashMap<String, String>();

    private ConcurrentHashMap<String, String> targetTopic = new ConcurrentHashMap<String, String>();

    private DelayQueue<DelayItem> delayQueue = new DelayQueue<DelayItem>();

    private ConcurrentHashMap<String, String> fullPullerSchema = new ConcurrentHashMap<String, String>();

    private ReentrantLock fullPullerLock = new ReentrantLock();

    private EventContainer() {
    }

    public static EventContainer getInstances() {
        if (container == null) {
            synchronized (EventContainer.class) {
                if (container == null)
                    container = new EventContainer();
            }
        }
        return container;
    }

    public void put(IEvent key, Thread value) {
        cmp.put(key, value);
    }

    public void stop() {
        //礼貌通知退出
        Enumeration<IEvent> keys = cmp.keys();
        while (keys.hasMoreElements()) {
            IEvent event = keys.nextElement();
            Thread thread = cmp.get(event);
            event.stop();
        }

        sleep(3L, TimeUnit.SECONDS);

        //强制停止
        keys = cmp.keys();
        while (keys.hasMoreElements()) {
            IEvent event = keys.nextElement();
            Thread thread = cmp.get(event);
            if (thread.getState() != Thread.State.TERMINATED && !thread.isInterrupted()) {
                thread.interrupt();
            }
        }
    }

    public void clear() {
        cmp.clear();
        skipSchema.clear();
        targetTopic.clear();
        delayQueue.clear();
    }

    private void sleep(long t, TimeUnit tu) {
        try {
            tu.sleep(t);
        } catch (InterruptedException e) {
            LoggerFactory.getLogger().error("[kafka-consumer-container] 线程sleep:" + t + " " + tu.name() + "中被中断!");
        }
    }

    public void offerDealyItem(DelayItem item) {
        delayQueue.offer(item);
    }

    public DelayItem takeDealyItem() throws InterruptedException {
        return delayQueue.take();
    }

    public Iterator<DelayItem> dqIt() {
        return delayQueue.iterator();
    }

    public void putSkipSchema(String schema) {
        skipSchema.put(StringUtils.replace(schema, ".", "/"), schema);
    }

    public void removeSkipSchema(String schema) {
        skipSchema.remove(StringUtils.replace(schema, ".", "/"));
    }

    public void putFullPullerSchema(String schema) {
        fullPullerSchema.put(schema, schema);
    }

    public void removeFullPullerSchema(String schema) {
        fullPullerSchema.remove(schema);
    }

    public void fullPullerLock() {
        fullPullerLock.lock();
    }

    public void fullPullerUnLock() {
        fullPullerLock.unlock();
    }

    public boolean containsFullPullerSchema(String schema) {
        return fullPullerSchema.containsKey(schema);
    }

    public String getSkipSchema(String schema) {
        return skipSchema.get(StringUtils.replace(schema, ".", "/"));
    }

    public void putSkipTargetTopic(String topic) {
        targetTopic.put(topic, topic);
    }

    public void removeSkipTargetTopic(String topic) {
        targetTopic.remove(topic);
    }

    public String getSkipTargetTopic(String topic) {
        return targetTopic.get(topic);
    }

    /**
     * 获取同一个目标topic下的所有schema，除了停止拉全量的schema的心跳检查之外，
     * 还需停止与此schmea处于同一topic下的所有schema的心跳检查，
     * 因为这些schema也会受拉全量的影响，
     * 主要原因是由于kafka消费能力有限
     *
     * @param schema
     * @return
     */
    public Set<String> getSchemasOfTargetTopic(String schema) {
        if (StringUtils.isEmpty(schema))
            return null;
        Set<TargetTopicVo> topics = HeartBeatConfigContainer.getInstance().getTargetTopic();
        Set<String> targetTopic = new HashSet<String>();
        for (TargetTopicVo topic : topics) {
            String dbSchema = StringUtils.join(new String[]{topic.getDsName(), topic.getSchemaName()}, "/");
            if (StringUtils.replace(schema, ".", "/").equals(dbSchema)) {
                targetTopic.add(topic.getTargetTopic());
            }
        }
        Set<String> set = new HashSet<String>();
        set.add(schema);
        if (targetTopic.size() == 0)
            return set;

        for (TargetTopicVo topic : topics) {
            if (targetTopic.contains(topic.getTargetTopic())) {
                String dbSchema = StringUtils.join(new String[]{topic.getDsName(), topic.getSchemaName()}, "/");
                set.add(dbSchema);
            }
        }
        return set;
    }

    public Set<String> getTargetTopic(String schema) {
        if (StringUtils.isEmpty(schema))
            return null;
        Set<TargetTopicVo> topics = HeartBeatConfigContainer.getInstance().getTargetTopic();
        Set<String> targetTopic = new HashSet<String>();
        for (TargetTopicVo topic : topics) {
            String dbSchema = StringUtils.join(new String[]{topic.getDsName(), topic.getSchemaName()}, "/");
            if (StringUtils.replace(schema, ".", "/").equals(dbSchema)) {
                targetTopic.add(topic.getTargetTopic());
            }
        }
        return targetTopic;
    }

    /*
     * 在拉全量完成后，更新此schema下的所有节点，由于在拉取全量过程中某些表的时间一直未更新，以防全量拉取一结束，检查心跳就会报警。
     */
    public void updateZkNodeAfterFullPulling(String dbSchema) {
        if (StringUtils.isEmpty(dbSchema))
            return;
        Set<MonitorNodeVo> nodes = HeartBeatConfigContainer.getInstance().getMonitorNodes();
        for (MonitorNodeVo node : nodes) {
            if (StringUtils.isEmpty(node.getSchema()) || StringUtils.isEmpty(node.getDsName()))
                continue;
            String db_schema = node.getDsName() + "/" + node.getSchema();
            if (!db_schema.equals(dbSchema))
                continue;
            String path = HeartBeatConfigContainer.getInstance().getHbConf().getMonitorPath();
            path = StringUtils.join(new String[]{path, node.getDsName(), node.getSchema(), node.getTableName()}, "/");
            try {
                PacketVo packet = deserialize(path, PacketVo.class);
                if (packet == null)
                    continue;
                long time = System.currentTimeMillis();
                packet.setTime(time);
                saveZk(path, JsonUtil.toJson(packet));
            } catch (Exception e) {
                // TODO Auto-generated catch block
                LOG.warn("[EventContainer] deserialize failed!", e);
            }
        }
    }

    /**
     * 在拉完全量后将此schema的kafka consumer的offset设置为最新
     *
     * @param dbSchema
     */
    /*public void setKafkaOffsetToLargest(String targetTopic){
    	if(targetTopic==null)
    		return;
    	TopicPartition partition0 = new TopicPartition(targetTopic, 0);
    	KafkaConsumerContainer.getInstances().getConsumer(targetTopic).seekToEnd(Arrays.asList(partition0));
    }*/
    protected <T> T deserialize(String path, Class<T> clazz) throws Exception {
        T packet = null;
        CuratorFramework curator = CuratorContainer.getInstance().getCurator();
        if (curator.getState() == CuratorFrameworkState.STOPPED) {
            LOG.info("[EventContainer] CuratorFrameworkState:{}", CuratorFrameworkState.STOPPED.name());
        } else {
            byte[] bytes = curator.getData().forPath(path);
            if (bytes != null && bytes.length != 0) {
                packet = JsonUtil.fromJson(new String(bytes, Charset.forName("UTF-8")), clazz);
            }
        }
        return packet;
    }

    protected void saveZk(String node, String packet) {
        try {
            CuratorFramework curator = CuratorContainer.getInstance().getCurator();
            if (curator.getState() == CuratorFrameworkState.STOPPED) {
                LOG.info("[EventContainer] CuratorFrameworkState:{}", CuratorFrameworkState.STOPPED.name());
            } else {
                curator.setData().forPath(node, packet.getBytes());
            }
        } catch (Exception e) {
            LOG.error("[control-event] 报错znode: " + node + ",数据包:" + packet + "失败!", e);
        }
    }
}
