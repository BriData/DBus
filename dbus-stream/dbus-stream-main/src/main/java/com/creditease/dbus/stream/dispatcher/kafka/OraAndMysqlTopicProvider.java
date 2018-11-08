package com.creditease.dbus.stream.dispatcher.kafka;


import com.creditease.dbus.stream.common.DataSourceInfo;
import com.creditease.dbus.stream.common.appender.kafka.TopicProvider;
import com.google.common.collect.Lists;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by zhenlinzhong on 2018/4/24.
 */
public class OraAndMysqlTopicProvider implements TopicProvider {

    private DataSourceInfo dsInfo;

    public OraAndMysqlTopicProvider(DataSourceInfo dsInfo){
        this.dsInfo = dsInfo;
    }

    @Override
    public List<String> provideTopics(Object... args) {
        Set<String> topics = new HashSet<>();
        topics.add(dsInfo.getCtrlTopic());
        topics.add(dsInfo.getDataTopic());
        return Lists.newArrayList(topics);
    }
}
