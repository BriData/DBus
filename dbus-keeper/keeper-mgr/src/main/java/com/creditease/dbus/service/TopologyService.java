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


package com.creditease.dbus.service;

import com.creditease.dbus.bean.Topology;
import com.creditease.dbus.commons.IZkService;
import com.creditease.dbus.constant.KeeperConstants;
import com.creditease.dbus.utils.SSHUtils;
import com.creditease.dbus.utils.StormToplogyOpHelper;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

/**
 * This is Description
 *
 * @author xiancangao
 * @date 2019/01/30
 */
@Service
public class TopologyService {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private IZkService zkService;
    @Autowired
    private Environment env;
    @Autowired
    private StormToplogyOpHelper stormTopoHelper;

    @Async
    public void batchRestartTopo(String jarPath, String topoType, ArrayList<String> dsNameList) throws Exception {
        Properties globalConf = zkService.getProperties(KeeperConstants.GLOBAL_CONF);
        String hostIp = globalConf.getProperty(KeeperConstants.GLOBAL_CONF_KEY_STORM_NIMBUS_HOST);
        String port = globalConf.getProperty(KeeperConstants.GLOBAL_CONF_KEY_CLUSTER_SERVER_SSH_PORT);
        String stormBaseDir = globalConf.getProperty(KeeperConstants.GLOBAL_CONF_KEY_STORM_NIMBUS_HOME_PATH);
        String stormSshUser = globalConf.getProperty(KeeperConstants.GLOBAL_CONF_KEY_CLUSTER_SERVER_SSH_USER);

        HashMap<String, String> runningTopos = new HashMap<>();
        for (Topology topology : stormTopoHelper.getAllTopologiesInfo()) {
            runningTopos.put(topology.getName(), topology.getId());
        }
        logger.info("running topology list :{}", runningTopos);
        dsNameList.forEach(dsName -> {
            try {
                String topoId = runningTopos.get(dsName + "-" + topoType);
                if (StringUtils.isNotBlank(topoId)) {
                    logger.info("find running topology {}", topoId);
                    String killResult = stormTopoHelper.stopTopology(topoId, 5, env.getProperty("pubKeyPath"));
                    if ("error".equals(killResult)) {
                        logger.error("Exception when kill topology {} ", topoId);
                    }
                    //等待15秒,防止没有kill掉
                    try {
                        Thread.sleep(15000);
                    } catch (InterruptedException e) {
                        logger.error("error when wait for topo {} killed", topoId);
                    }
                    boolean killed = true;
                    while (killed) {
                        killed = false;
                        for (Topology topology : stormTopoHelper.getAllTopologiesInfo()) {
                            String name = topology.getName();
                            if (name.equals(dsName + "-" + topoType)) {
                                try {
                                    Thread.sleep(5000);
                                } catch (InterruptedException e) {
                                    logger.error("error when wait for topo {} killed", topoId);
                                }
                                killed = true;
                                logger.info("wait for killing running topology {}", topoId);
                                break;
                            }
                        }
                    }
                    logger.info("kill running topology {} success", topoId);
                }
                String cmd = "cd " + stormBaseDir + "/" + KeeperConstants.STORM_JAR_DIR + ";";
                cmd += " ./dbus_startTopology.sh " + stormBaseDir + " " + topoType + " " + env.getProperty("zk.str");
                cmd += " " + dsName + " " + jarPath;
                logger.info("Topology Start Command:{}", cmd);
                String errorMsg = SSHUtils.executeCommand(stormSshUser, hostIp, Integer.parseInt(port), env.getProperty("pubKeyPath"), cmd, true);
                if (StringUtils.isNotBlank(errorMsg)) {
                    logger.error("Exception when start topology {} ", dsName + topoType);
                }
                logger.info("start topology {} success", dsName + topoType);
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    logger.error("error when wait for topo {} start.", dsName + "-" + topoType);
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        });
        logger.info("*************  batch restart topo end ************* ");
        //final CountDownLatch latch = new CountDownLatch(dsNameList.size());
        //dsNameList.forEach(dsName -> {
        //    new Thread(() -> {
        //        try {
        //            String topoId = runningTopos.get(dsName + "-" + topoType);
        //            if (StringUtils.isNotBlank(topoId)) {
        //                logger.info("find running topology {}", topoId);
        //                String killResult = stormTopoHelper.stopTopology(topoId, 10, env.getProperty("pubKeyPath"));
        //                if ("error".equals(killResult)) {
        //                    logger.error("Exception when kill topology {} ", topoId);
        //                }
        //                //等待5秒,防止没有kill掉
        //                try {
        //                    Thread.sleep(5000);
        //                } catch (InterruptedException e) {
        //                    logger.error("error when wait for topo killed");
        //                }
        //                boolean killed = true;
        //                while (killed) {
        //                    killed = false;
        //                    for (Topology topology : stormTopoHelper.getAllTopologiesInfo()) {
        //                        String name = topology.getName();
        //                        if (name.equals(dsName + "-" + topoType)) {
        //                            try {
        //                                Thread.sleep(5000);
        //                            } catch (InterruptedException e) {
        //                                logger.error("error when wait for topo killed");
        //                            }
        //                            killed = true;
        //                            logger.info("wait for killing running topology {}", topoId);
        //                            break;
        //                        }
        //                    }
        //                }
        //                logger.info("kill running topology {} success", topoId);
        //            }
        //            String cmd = "cd " + stormBaseDir + "/" + KeeperConstants.STORM_JAR_DIR + ";";
        //            cmd += " ./dbus_startTopology.sh " + stormBaseDir + " " + topoType + " " + env.getProperty("zk.str");
        //            cmd += " " + dsName + " " + jarPath;
        //            logger.info("Topology Start Command:{}", cmd);
        //            String errorMsg = SSHUtils.executeCommand(stormSshUser, hostIp, Integer.parseInt(port), env.getProperty("pubKeyPath"), cmd, true);
        //            if (StringUtils.isNotBlank(errorMsg)) {
        //                logger.error("Exception when start topology {} ", dsName + topoType);
        //            }
        //            logger.info("start topology {} success", dsName + topoType);
        //            latch.countDown();
        //        } catch (Exception e) {
        //            logger.error(e.getMessage(), e);
        //        }
        //    }).start();
        //});
        //try {
        //    latch.await();
        //    logger.info("*************  batch restart topo end ************* ");
        //} catch (InterruptedException e) {
        //    logger.error(e.getMessage(), e);
        //}
    }

}
