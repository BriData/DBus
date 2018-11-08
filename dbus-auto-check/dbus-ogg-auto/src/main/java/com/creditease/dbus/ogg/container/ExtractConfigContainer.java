package com.creditease.dbus.ogg.container;

import com.creditease.dbus.ogg.bean.ExtractConfigBean;

/**
 * User: 王少楠
 * Date: 2018-08-28
 * Desc:
 */
public class ExtractConfigContainer {
    private ExtractConfigBean extrConfig;

    private static ExtractConfigContainer container;

    private ExtractConfigContainer(){}

    public static ExtractConfigContainer getInstance(){
        if(container == null){
            synchronized (ExtractConfigContainer.class){
                if(container == null)
                    container = new ExtractConfigContainer();
            }
        }
        return container;
    }

    public ExtractConfigBean getExtrConfig(){
        return extrConfig;
    }

    public void setExtrConfig(ExtractConfigBean extrConfig){
        this.extrConfig=extrConfig;
    }
}
