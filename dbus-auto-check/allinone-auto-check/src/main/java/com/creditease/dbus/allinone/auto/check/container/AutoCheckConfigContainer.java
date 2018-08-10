package com.creditease.dbus.allinone.auto.check.container;

import com.creditease.dbus.allinone.auto.check.bean.AutoCheckConfigBean;


public class AutoCheckConfigContainer {

    private static AutoCheckConfigContainer container;

    private AutoCheckConfigBean autoCheckConf;

    private AutoCheckConfigContainer() {
    }

    public static AutoCheckConfigContainer getInstance() {
        if (container == null) {
            synchronized (AutoCheckConfigContainer.class) {
                if (container == null)
                    container = new AutoCheckConfigContainer();
            }
        }
        return container;
    }

    public AutoCheckConfigBean getAutoCheckConf() {
        return autoCheckConf;
    }

    public void setAutoCheckConf(AutoCheckConfigBean autoCheckConf) {
        this.autoCheckConf = autoCheckConf;
    }

    public void clear() {
        autoCheckConf = null;
    }
}
