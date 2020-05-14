package com.creditease.dbus.heartbeat.distributed.data;

import java.util.Map;


public interface Convert {

    void configure(Map<String, ?> configs, boolean isKey);

    byte[] fromtData(String topic, Schema schema, Object value);

    SchemaAndValue toData(String topic, byte[] value);

}
