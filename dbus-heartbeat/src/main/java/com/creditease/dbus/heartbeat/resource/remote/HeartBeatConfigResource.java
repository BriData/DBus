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


package com.creditease.dbus.heartbeat.resource.remote;

import com.creditease.dbus.heartbeat.container.HeartBeatConfigContainer;
import com.creditease.dbus.heartbeat.log.LoggerFactory;
import com.creditease.dbus.heartbeat.util.JsonUtil;
import com.creditease.dbus.heartbeat.vo.HeartBeatVo;
import org.apache.commons.lang.StringUtils;

import java.nio.charset.Charset;

public class HeartBeatConfigResource extends ZkConfigResource<HeartBeatVo> {

    public HeartBeatConfigResource() {
        this(StringUtils.EMPTY);
    }

    protected HeartBeatConfigResource(String name) {
        super(name);
    }

    @Override
    public HeartBeatVo parse() {
        String path = StringUtils.EMPTY;
        HeartBeatVo hbvo = new HeartBeatVo();
        try {
            path = HeartBeatConfigContainer.getInstance().getZkConf().getConfigPath();
            LoggerFactory.getLogger().info("path........" + path);
            byte[] bytes = curator.getData().forPath(path);
            if (bytes == null || bytes.length == 0) {
                throw new RuntimeException("[load-zk-config] 加载zk path: " + path + "配置信息不存在.");
            }
            String tmp = new String(bytes, Charset.forName("UTF-8"));
            hbvo = JsonUtil.fromJson(tmp, HeartBeatVo.class);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("[load-zk-config] 加载zk path: " + path + "配置信息出错.", e);
        }
        return hbvo;
    }

}
