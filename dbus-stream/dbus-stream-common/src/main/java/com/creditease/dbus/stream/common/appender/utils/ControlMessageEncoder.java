/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2018 Bridata
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

package com.creditease.dbus.stream.common.appender.utils;

import com.creditease.dbus.commons.ControlMessage;
import com.creditease.dbus.commons.DBusConsumerRecord;
import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandlerHelper;
import com.creditease.dbus.stream.common.tools.DateUtil;
import org.apache.avro.generic.GenericRecord;

/**
 * Created by Shrimp on 16/6/17.
 */
public class ControlMessageEncoder {
    private static final String FULL_DATA_PULL_REQ = "FULL_DATA_PULL_REQ";

    public static ControlMessage fullDataPollMessage(GenericRecord record, String topologyId, DBusConsumerRecord<String, byte[]> consumerRecord) {
        ControlMessage message = new ControlMessage();
        message.setId(System.currentTimeMillis());
        message.setFrom(topologyId);
        message.setType(FULL_DATA_PULL_REQ);
        message.addPayload("topic", consumerRecord.topic());
        message.addPayload("DBUS_DATASOURCE_ID", Utils.getDatasource().getId());
        PairWrapper<String, Object> wrapper = BoltCommandHandlerHelper.convertAvroRecord(record, Constants.MessageBodyKey.noorderKeys);
        message.addPayload("OP_TS", wrapper.getProperties(Constants.MessageBodyKey.OP_TS).toString());
        message.addPayload("POS", wrapper.getProperties(Constants.MessageBodyKey.POS).toString());
        for (Pair<String,Object> pair : wrapper.getPairs()) {
            message.addPayload(pair.getKey(), pair.getValue());
        }

        return message;
    }

}
