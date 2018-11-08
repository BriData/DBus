package com.creditease.dbus.ogg.container;

import com.creditease.dbus.ogg.bean.ConfigBean;

/**
 * User: 王少楠
 * Date: 2018-08-24
 * Desc:
 */
public class AutoCheckConfigContainer {

    private static AutoCheckConfigContainer container;

    private ConfigBean configBean;

    private AutoCheckConfigContainer(){

    }

    public static AutoCheckConfigContainer getInstance(){
        if(container == null){
            synchronized (AutoCheckConfigContainer.class){
                if(container == null){
                    container = new AutoCheckConfigContainer();
                }
            }
        }
        return container;
    }

    public ConfigBean getConfig(){
        return configBean;
    }
    public void setConfig(ConfigBean config){
        configBean = config;
    }

    public void clear(){
        configBean = null;
    }
}
