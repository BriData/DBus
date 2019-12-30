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


package com.creditease.dbus.stream.appender.utils;

import com.alibaba.fastjson.JSON;
import com.creditease.dbus.commons.IZkService;
import com.creditease.dbus.commons.ZkService;

/**
 * Created by Shrimp on 16/6/29.
 */
public abstract class ZKNodeOperator {
    private IZkService zk;
    private String zkNode;

    public ZKNodeOperator(String zkconnect, String zkNode) throws Exception {
        this(new ZkService(zkconnect), zkNode);
    }

    public ZKNodeOperator(IZkService zk, String zkNode) {
        this.zk = zk;
        this.zkNode = zkNode;
    }

    public void createWhenNotExists(Object initializeValue) throws Exception {
        if (!zk.isExists(zkNode)) {
            zk.createNode(zkNode, getJsonBytes(initializeValue));
        }
    }

    public byte[] getBytesData() throws Exception {
        return zk.getData(zkNode);
    }

    public <T> T getData() throws Exception {
        return convert(getBytesData());
    }

    public abstract <T> T convert(byte[] data);

    public void setData(Object payload, boolean createWhenNotExists) throws Exception {
        if (!zk.isExists(zkNode)) {
            if (createWhenNotExists) {
                zk.createNode(zkNode, getJsonBytes(payload));
            }
        } else {
            zk.setData(zkNode, getJsonBytes(payload));
        }
    }

    private byte[] getJsonBytes(Object obj) {
        if (obj != null && !(obj instanceof CharSequence)) {
            return JSON.toJSONString(obj).getBytes();
        }
        if (obj != null && (obj instanceof Byte[] || obj instanceof Byte[])) {
            return (byte[]) obj;
        }
        return obj.toString().getBytes();
    }


    public static void main(String[] args) {
        Object bytes = new Byte[]{1};
        System.out.println(bytes instanceof byte[]);
    }
}
