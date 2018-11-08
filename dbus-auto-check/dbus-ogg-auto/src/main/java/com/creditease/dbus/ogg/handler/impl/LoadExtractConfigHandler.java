package com.creditease.dbus.ogg.handler.impl;

import com.creditease.dbus.ogg.bean.ExtractConfigBean;
import com.creditease.dbus.ogg.container.ExtractConfigContainer;
import com.creditease.dbus.ogg.handler.AbstractHandler;
import com.creditease.dbus.ogg.resource.IResource;
import com.creditease.dbus.ogg.resource.impl.ExtractConfigResource;

import java.io.BufferedWriter;
import java.io.File;

/**
 * User: 王少楠
 * Date: 2018-08-28
 * Desc:
 */
public class LoadExtractConfigHandler extends AbstractHandler {

    public void checkDeploy(BufferedWriter bw) throws Exception {
        loadExtractConfig(bw);
    }
    private void loadExtractConfig(BufferedWriter bw) throws Exception{
        System.out.println("============================================");
        System.out.println("加载extract 配置文件");
        bw.write("============================================");
        bw.newLine();
        bw.write("加载extract 配置文件");
        bw.newLine();

        try{

            IResource<ExtractConfigBean> resource = new ExtractConfigResource("ogg-auto-extract.properties");
            ExtractConfigBean extractConfig = resource.load();
            if(!validateConfig(extractConfig)){
                String errMsg = "请检查配置项：[ogg.home: "+extractConfig.getOggHome()+"]";
                bw.write(errMsg);
                bw.newLine();
                System.out.println(errMsg);
                throw new Exception(errMsg);
            }
            ExtractConfigContainer.getInstance().setExtrConfig(extractConfig);
        }catch (Exception e){
            bw.write("加载失败");
            bw.newLine();
            System.out.println("加载失败");
        }
    }

    private boolean validateConfig(ExtractConfigBean extractConfig){
        boolean result;
        //check home路径正确性，其他不做检查
        String oggHome = extractConfig.getOggHome();
        File file = new File(oggHome,"dirprm");
        result = file.exists();
        return result;
    }
}
