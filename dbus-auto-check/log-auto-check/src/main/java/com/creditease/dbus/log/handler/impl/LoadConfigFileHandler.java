
package com.creditease.dbus.log.handler.impl;


import com.creditease.dbus.log.bean.LogCheckConfigBean;
import com.creditease.dbus.log.container.AutoCheckConfigContainer;
import com.creditease.dbus.log.handler.AbstractHandler;
import com.creditease.dbus.log.resource.IResource;
import com.creditease.dbus.log.resource.local.AutoCheckFileConfigResource;

import java.io.BufferedWriter;

public class LoadConfigFileHandler extends AbstractHandler {

    @Override
    public void check(BufferedWriter bw) throws Exception {
         loadAutoCheckConfig();
    }

    @Override
    public void deploy(BufferedWriter bw) throws Exception {
        loadAutoCheckConfig();
    }

    private void loadAutoCheckConfig() {
        IResource<LogCheckConfigBean> resource = new AutoCheckFileConfigResource("log-conf.properties");
        LogCheckConfigBean conf = resource.load();
        AutoCheckConfigContainer.getInstance().setAutoCheckConf(conf);
    }

}
