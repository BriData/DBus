package com.creditease.dbus.ogg.resource.impl;

import com.creditease.dbus.ogg.bean.ExtractConfigBean;
import com.creditease.dbus.ogg.resource.AbstractConfigResource;
import org.apache.commons.lang3.StringUtils;

/**
 * User: 王少楠
 * Date: 2018-08-28
 * Desc:
 */
public class ExtractConfigResource extends AbstractConfigResource<ExtractConfigBean> {
    public ExtractConfigResource(String name) {
        super(name);
    }

    public ExtractConfigBean parse() {
        ExtractConfigBean extractConfig = new ExtractConfigBean();
        try{
            //加载初始化的配置信息
            if(!StringUtils.isBlank(props.getProperty("tables.append"))){
                String[] appendTables = props.getProperty("tables.append").trim().split(",");
                extractConfig.setAppendTables(appendTables);
                extractConfig.setExtrName(props.getProperty("extract.name").trim());
                extractConfig.setOggHome(props.getProperty("ogg.home").trim());
            }else {
                extractConfig.setOggUser(props.getProperty("ogg.user").trim());
                extractConfig.setOggPwd(props.getProperty("ogg.pwd").trim());
                extractConfig.setOggHome(props.getProperty("ogg.home").trim());
                extractConfig.setExtrName(props.getProperty("extract.name").trim());
                extractConfig.setRmHost(props.getProperty("rm.host").trim());
                extractConfig.setMgrPort(props.getProperty("mgr.port").trim());
                extractConfig.setExtractFile(props.getProperty("extract.file").trim());
                String tables = props.getProperty("tables").trim();
                tables+=",DBUS.DB_FULL_PULL_REQUESTS" +
                        ",DBUS.DB_HEARTBEAT_MONITOR" +
                        ",DBUS.META_SYNC_EVENT" +
                        ",DBUS.TEST_TABLE";
                extractConfig.setTables(tables.split(","));
                extractConfig.setNlsLang(props.getProperty("nls.lang").trim());
            }
            return extractConfig;
        }catch (Exception e){
            System.out.println("加载失败...");
            throw e;
        }
    }

}
