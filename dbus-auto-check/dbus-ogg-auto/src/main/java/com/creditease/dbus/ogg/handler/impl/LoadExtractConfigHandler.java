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

import com.creditease.dbus.ogg.bean.ExtractConfigBean;
import com.creditease.dbus.ogg.container.ExtractConfigContainer;
import com.creditease.dbus.ogg.handler.AbstractHandler;
import com.creditease.dbus.ogg.resource.IResource;
import com.creditease.dbus.ogg.resource.impl.ExtractConfigResource;

import java.io.BufferedWriter;
import java.io.File;

import static com.creditease.dbus.ogg.utils.FileUtil.writeAndPrint;

/**
 * User: 王少楠
 * Date: 2018-08-28
 * Desc:
 */
public class LoadExtractConfigHandler extends AbstractHandler {

    public void checkDeploy(BufferedWriter bw) throws Exception {
        loadExtractConfig(bw);
    }

    private void loadExtractConfig(BufferedWriter bw) throws Exception {
        writeAndPrint("加载 extract 配置文件 ...");


        try {

            IResource<ExtractConfigBean> resource = new ExtractConfigResource("ogg-auto-extract.properties");
            ExtractConfigBean extractConfig = resource.load();
            if (!validateConfig(extractConfig)) {
                writeAndPrint("请检查配置项：[ogg.home: " + extractConfig.getOggHome() + "]");

                throw new Exception();
            }
            ExtractConfigContainer.getInstance().setExtrConfig(extractConfig);
            writeAndPrint("加载 extract 配置文件完成");

        } catch (Exception e) {
            writeAndPrint("加载 extract 配置文件失败,请检查配置项");
            throw e;
        }
    }

    private boolean validateConfig(ExtractConfigBean extractConfig) {
        boolean result;
        //check home路径正确性，其他不做检查
        String oggHome = extractConfig.getOggHome();
        File file = new File(oggHome, "dirprm");
        result = file.exists();
        return result;
    }
}
