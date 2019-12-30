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


package com.creditease.dbus.heartbeat.type;

import com.alibaba.fastjson.JSON;
import com.creditease.dbus.commons.ControlVo;
import com.creditease.dbus.commons.CtlMessageResult;
import com.creditease.dbus.heartbeat.container.*;
import com.creditease.dbus.heartbeat.log.LoggerFactory;
import com.creditease.dbus.heartbeat.util.JsonUtil;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.util.Set;

/**
 * 通过监控zookeeper节点(/dbus/heartbeat/control)数据变化,实现如下控制:
 * 1. 重新加载配置
 * 2. 停止心跳,全量拉取检查,心跳检查
 * 3. 启动心跳,全量拉取检查,心跳检查
 * 4. 停止整个监控进程
 * 5. 拉取全量
 * <p>
 * 控制参数JSON格式: {"cmdType":0,"args":"cm"}
 *
 * @author Liang.Ma
 * @version 1.0
 */
public enum WatcherType implements Watcher {
    CONTROL {
        @Override
        public void process(WatchedEvent event) {
            switch (event.getType()) {
                case NodeDataChanged: {
                    try {
                        CuratorFramework _curator = CuratorContainer.getInstance().getCurator();
                        byte[] bytes = _curator.getData().usingWatcher(WatcherType.CONTROL).forPath(event.getPath());
                        ControlVo ctrlVo = JsonUtil.fromJson(new String(bytes), ControlVo.class);
                        CommandType cmdType = CommandType.fromInt(ctrlVo.getCmdType());
                        switch (cmdType) {
                            case RELOAD:
                                reload(cmdType, ctrlVo);
                                break;
                            case STOP:
                                stop();
                                break;
                            case START:
                                start(cmdType);
                                break;
                            case DESTORY:
                                destory();
                                break;
                            case FULL_PULLER_BEGIN:
                                fullPullerBegin(ctrlVo);
                                break;
                            case FULL_PULLER_END:
                                fullPullerEnd(ctrlVo);
                                break;
                            default:
                                break;
                        }
                    } catch (Exception e) {
                        LoggerFactory.getLogger().error("[command-control]", e);
                    }
                    break;
                }
                default:
                    break;
            }
        }
    };

    private static void destory() {
        LoggerFactory.getLogger().info("[command-control] 开始关闭心跳进程.");
        CuratorContainer.getInstance().close();
        DataSourceContainer.getInstance().clear();
        EventContainer.getInstances().stop();
        EventContainer.getInstances().clear();
        KafkaConsumerContainer.getInstances().shutdown();
        LifeCycleContainer.getInstance().stop();
        LifeCycleContainer.getInstance().clear();
        LoggerFactory.getLogger().info("[command-control] 完成关闭心跳进程.");
    }

    private static void start(CommandType cmdType) {
        LoggerFactory.getLogger().info("[command-control] 开始启动心跳发送.");
        cmdType.exec();
        LoggerFactory.getLogger().info("[command-control] 完成启动心跳发送.");
    }

    private static void stop() {
        LoggerFactory.getLogger().info("[command-control] 开始停止心跳发送.");
        EventContainer.getInstances().stop();
        EventContainer.getInstances().clear();
        LoggerFactory.getLogger().info("[command-control] 完成停止心跳发送.");
    }

    private static void reload(CommandType cmdType, ControlVo ctrlVo) {
        CtlMessageResult rst = new CtlMessageResult("heart-beat", JSON.toJSONString(ctrlVo));
        try {
            LoggerFactory.getLogger().info("[command-control] 开始重新加载配置信息.");
            EventContainer.getInstances().stop();
            EventContainer.getInstances().clear();
            AlarmResultContainer.getInstance().clear();
            CuratorContainer.getInstance().close();
            DataSourceContainer.getInstance().clear();
            HeartBeatConfigContainer.getInstance().clear();
            KafkaConsumerContainer.getInstances().shutdown();
            cmdType.exec();
            LoggerFactory.getLogger().info("[command-control] 完成重新加载配置信息.");
        } catch (Exception e) {
            LoggerFactory.getLogger().error("[command-control] 重新加载配置信息报错.", e);
            rst.setMessage(e.getMessage());
        }
        // 回写Control Message Result
        try {
            String path = "/DBus/ControlMessageResult/HEARTBEAT_RELOAD";
            CuratorContainer.getInstance().createZkNode(path);
            byte[] data = JSON.toJSONString(rst, true).getBytes();
            CuratorContainer.getInstance().getCurator().setData().forPath(path, data);
        } catch (Exception e) {
            LoggerFactory.getLogger().error("[command-control] 回写心跳reload控制信息结果报错.", e);
        }
    }

    private static void fullPullerBegin(ControlVo ctrlVo) {
        LoggerFactory.getLogger().info("[command-control] 开始执行开启全量拉取通知.");
        LoggerFactory.getLogger().info("[command-control] 正在拉取全量的schema:{}", ctrlVo.getArgs());
        EventContainer.getInstances().putFullPullerSchema(ctrlVo.getArgs());
        Set<String> schemas = EventContainer.getInstances().getSchemasOfTargetTopic(ctrlVo.getArgs());

        try {
            EventContainer.getInstances().fullPullerLock();
            for (String schema : schemas) {
                EventContainer.getInstances().putSkipSchema(schema);
                if (!schema.equals(ctrlVo.getArgs())) {
                    LoggerFactory.getLogger().info("[command-control] 心跳检查受影响的schema:{}", schema);
                }
            }
        } catch (Exception e) {
            LoggerFactory.getLogger().error("[command-control] ", e);
        } finally {
            EventContainer.getInstances().fullPullerUnLock();
        }

        Set<String> targetTopics = EventContainer.getInstances().getTargetTopic(ctrlVo.getArgs());
        for (String topic : targetTopics) {
            EventContainer.getInstances().putSkipTargetTopic(topic);
        }
        LoggerFactory.getLogger().info("[command-control] 完成执行开启全量拉取通知.");
    }

    private static void fullPullerEnd(ControlVo ctrlVo) {
        LoggerFactory.getLogger().info("[command-control] 开始执行关闭全量拉取通知.");
        LoggerFactory.getLogger().info("[command-control] 结束拉取全量的schema:{}", ctrlVo.getArgs());
        // Set<String> schemas = EventContainer.getInstances().getSchemasOfTargetTopic(ctrlVo.getArgs());
        Set<String> targetTopics = EventContainer.getInstances().getTargetTopic(ctrlVo.getArgs());
        for (String topic : targetTopics) {
            //    EventContainer.getInstance().setKafkaOffsetToLargest(targetTopic);//不能放在这儿，kafkaConsumer为非线程安全的
            EventContainer.getInstances().removeSkipTargetTopic(topic);
        }
        /*
        for(String schema : schemas){
            //EventContainer.getInstances().updateZkNodeAfterFullPulling(schema);
            EventContainer.getInstances().removeSkipSchema(schema);
        }*/

        // 全量结束之后,给增量一个补齐的延迟时间暂定10分钟
        long delayMs = HeartBeatConfigContainer.getInstance().getHbConf().getHeartBeatTimeout();
        DelayItem item = new DelayItem(delayMs, ctrlVo.getArgs());
        EventContainer.getInstances().offerDealyItem(item);
        EventContainer.getInstances().removeFullPullerSchema(ctrlVo.getArgs());

        LoggerFactory.getLogger().info("[command-control] 完成执行关闭全量拉取通知.");
    }

}
