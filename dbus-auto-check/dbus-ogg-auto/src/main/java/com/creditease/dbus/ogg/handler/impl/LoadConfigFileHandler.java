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


package com.creditease.dbus.ogg.handler.impl;

import com.creditease.dbus.ogg.bean.ConfigBean;
import com.creditease.dbus.ogg.container.AutoCheckConfigContainer;
import com.creditease.dbus.ogg.handler.AbstractHandler;
import com.creditease.dbus.ogg.resource.IResource;
import com.creditease.dbus.ogg.resource.impl.FileConfigResource;

import java.io.BufferedWriter;
import java.io.File;

import static com.creditease.dbus.ogg.utils.FileUtil.writeAndPrint;

/**
 * User: 王少楠
 * Date: 2018-08-24
 * Desc:
 */
public class LoadConfigFileHandler extends AbstractHandler {

    public void checkDeploy(BufferedWriter bw) throws Exception {
        loadOggAutoConfig(bw);
    }

    private void loadOggAutoConfig(BufferedWriter bw) throws Exception {
        try {
            IResource<ConfigBean> resource = new FileConfigResource("ogg-auto.properties");
            ConfigBean config = resource.load();
            //检查目录
            if (!validateConfig(config)) {
                String errMsg = "请检查配置项：[ogg.big.home: " + config.getOggBigHome() + "]";
                writeAndPrint(errMsg);

                System.out.println(errMsg);
                throw new Exception(errMsg);
            }
            AutoCheckConfigContainer.getInstance().setConfig(config);
        } catch (Exception e) {
            writeAndPrint("加载ogg-auto.properties 属性失败");

            System.out.println("加载ogg-auto.properties 属性失败");
            throw e;
        }
    }

    private boolean validateConfig(ConfigBean config) {
        boolean result;
        //check home路径正确性，其他不做检查
        String oggHome = config.getOggBigHome();
        File file = new File(oggHome, "dirprm");
        result = file.exists();
        return result;
    }
}
