package com.creditease.dbus.log.container;

import com.creditease.dbus.log.bean.LogCheckConfigBean;


public class AutoCheckConfigContainer {

    private static AutoCheckConfigContainer container;

    private LogCheckConfigBean logCheckConf;

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

    public LogCheckConfigBean getAutoCheckConf() {
        return logCheckConf;
    }

    public void setAutoCheckConf(LogCheckConfigBean logCheckConf) {
        this.logCheckConf = logCheckConf;
    }

    public void clear() {
        logCheckConf = null;
    }
}
