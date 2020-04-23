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


package com.creditease.dbus.tools;


import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.ZkNode;
import com.creditease.dbus.commons.ZkService;
import com.creditease.dbus.tools.common.ConfUtils;
import com.google.common.base.Strings;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by 201605240095 on 2016/7/25.
 */
public class ZkConfBatchModifier {
    private static Logger logger = LoggerFactory.getLogger(ZkConfBatchModifier.class);
    public static final String UTF8 = "utf-8";

    public static void main(String[] args) throws Exception {

        //if (!(args.length == 3 || args.length == 4)) {
        //    logger.info("Please provide legal parameters(the last arg is optional): baseNode oldString newString [checker]. Eg: /DBus zk1:2181 zk1:2181,zk2:2181 :2181");
        //    return;
        //}
        //logger.info(String.format("Try to replace %s by %s", args[1], args[2]));

        Properties props = ConfUtils.loadZKProperties();
        String zkServer = props.getProperty(Constants.ZOOKEEPER_SERVERS);
        if (Strings.isNullOrEmpty(zkServer)) {
            logger.error("Zookeeper server cannot be null!");
            return;
        }
        ZkService zkService = new ZkService(zkServer);
        List<String> canal = zkService.getChildren("/DBus/Canal");
        for (String c : canal) {
            List<String> cluster = zkService.getChildren(String.format("/DBus/Canal/%s/otter/canal/cluster", c));
            if (cluster == null || cluster.size() == 0) {
                continue;
            }
            String destinations = zkService.getChildren(String.format("/DBus/Canal/%s/otter/canal/destinations", c)).get(0);
            String filter = String.format("/DBus/Canal/%s/otter/canal/destinations/%s/1001/filter", c, destinations);
            if (zkService.isExists(filter)) {
                zkService.deleteNode(filter);
                System.out.println(String.format("delete %s success", filter));
            }
        }
        zkService.close();

        //String basePath = args[0];
        //String oldString = args[1];
        //String newString = args[2];
        //if (!basePath.startsWith(Constants.DBUS_ROOT)) {
        //    logger.error("The base path is not start with " + Constants.DBUS_ROOT + ". Please check!");
        //    return;
        //}
        //
        //String checker = null;
        //if (args.length == 4) {
        //    checker = args[3];
        //}
        //
        //ZkService zkService = new ZkService(zkServer);
        //
        //ZkNode baseNode = new ZkNode(basePath); // Name name name Todo not start with  refuse; 直接搞指定path底下的。
        //baseNode.setPath(basePath);
        //
        //
        //byte[] data = zkService.getData(baseNode.getPath());
        //if (data != null && data.length > 0) {
        //    String orignialContent = new String(data, UTF8);
        //
        //    if (StringUtils.isNotBlank(checker) && orignialContent.indexOf(oldString) == -1 && orignialContent.indexOf(checker) != -1) {
        //        logger.warn(String.format("Found checker --- %s, But not found %s at %s", checker, oldString, baseNode.getPath()));
        //    }
        //
        //    if (orignialContent.indexOf(oldString) != -1) {
        //        logger.info(String.format("Found  %s at %s in \n%s", oldString, baseNode.getPath(), orignialContent));
        //        String newContent = orignialContent.replace(oldString, newString);
        //        zkService.setData(baseNode.getPath(), newContent.getBytes(UTF8));
        //    }
        //
        //}
        //
        //replaceContentRecursively(zkService, baseNode, oldString, newString, checker);
        //zkService.close();
    }


    private static List<ZkNode> replaceContentRecursively(ZkService zkService, ZkNode parent, String oldString, String newString, String checker) {
        List<ZkNode> childrenNodes = new ArrayList();
        List<String> children = null;
        try {
            children = zkService.getChildren(parent.getPath());
            if (children.size() > 0) {
                for (String nodeName : children) {
                    ZkNode node = new ZkNode(nodeName);
                    String path = parent.getPath() + "/" + nodeName;
                    if (parent.getPath().equals("/")) { //我们的地址是 /DBus/xxx的形式。需要对第一个 / 进行特殊处理.
                        path = parent.getPath() + nodeName;
                    }
                    node.setPath(path);
                    byte[] data = zkService.getData(path);
                    if (data != null && data.length > 0) {
                        String orignialContent = new String(data, UTF8);

                        if (StringUtils.isNotBlank(checker) && orignialContent.indexOf(oldString) == -1 && orignialContent.indexOf(checker) != -1) {
                            logger.warn(String.format("Found checker --- %s, But not found %s at %s", checker, oldString, path));
                        }

                        if (orignialContent.indexOf(oldString) != -1) {
                            logger.info(String.format("Found  %s at %s in \n%s", oldString, path, orignialContent));
                            String newContent = orignialContent.replace(oldString, newString);
                            zkService.setData(path, newContent.getBytes(UTF8));
                        }
                    }
                    childrenNodes.add(node);
                }

                for (ZkNode node : childrenNodes) {
                    replaceContentRecursively(zkService, node, oldString, newString, checker);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return childrenNodes;
    }

}
