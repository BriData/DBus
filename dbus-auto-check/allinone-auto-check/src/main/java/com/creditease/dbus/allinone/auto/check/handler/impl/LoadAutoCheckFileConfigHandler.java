
package com.creditease.dbus.allinone.auto.check.handler.impl;

import java.io.BufferedWriter;

import com.creditease.dbus.allinone.auto.check.bean.AutoCheckConfigBean;
import com.creditease.dbus.allinone.auto.check.container.AutoCheckConfigContainer;
import com.creditease.dbus.allinone.auto.check.handler.AbstractHandler;
import com.creditease.dbus.allinone.auto.check.resource.IResource;
import com.creditease.dbus.allinone.auto.check.resource.local.AutoCheckFileConfigResource;

public class LoadAutoCheckFileConfigHandler extends AbstractHandler {

    @Override
    public void check(BufferedWriter bw) throws Exception {
        loadAutoCheckConfig();
    }

    private void loadAutoCheckConfig() {
        IResource<AutoCheckConfigBean> resource = new AutoCheckFileConfigResource("auto-check-conf.properties");
        AutoCheckConfigBean conf = resource.load();
        AutoCheckConfigContainer.getInstance().setAutoCheckConf(conf);
    }

}
