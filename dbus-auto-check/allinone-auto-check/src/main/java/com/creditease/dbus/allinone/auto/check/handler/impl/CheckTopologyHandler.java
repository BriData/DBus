package com.creditease.dbus.allinone.auto.check.handler.impl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.allinone.auto.check.bean.AutoCheckConfigBean;
import com.creditease.dbus.allinone.auto.check.container.AutoCheckConfigContainer;
import com.creditease.dbus.allinone.auto.check.handler.AbstractHandler;
import com.creditease.dbus.allinone.auto.check.utils.MsgUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Administrator on 2018/8/1.
 */
public class CheckTopologyHandler extends AbstractHandler {

    private static Logger logger = LoggerFactory.getLogger(CheckTopologyHandler.class);

    @Override
    public void check(BufferedWriter bw) throws Exception {
        AutoCheckConfigBean conf = AutoCheckConfigContainer.getInstance().getAutoCheckConf();
        String stormUIAPI = conf.getStormUIApi() + "/topology/summary";
        bw.write("check topology start: ");
        bw.newLine();
        bw.write("api: " + stormUIAPI);
        bw.newLine();

        String strData = httpGet(stormUIAPI);

        JSONObject data = JSON.parseObject(strData);
        JSONArray topologies = data.getJSONArray("topologies");
        for (JSONObject topology : topologies.toJavaList(JSONObject.class)) {
            String name = topology.getString("name");
            String status = topology.getString("status");
            bw.write(MsgUtil.format("topology {0} status is {1}", name, status));
            bw.newLine();
        }
    }

    private String httpGet(String serverUrl) throws Exception {
        StringBuilder responseBuilder = null;
        BufferedReader reader = null;

        URL url;
        try {
            url = new URL(serverUrl);
            URLConnection conn = url.openConnection();
            conn.setDoOutput(true);
            conn.setConnectTimeout(1000 * 5);
            reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            responseBuilder = new StringBuilder();
            String line = null;
            while ((line = reader.readLine()) != null) {
                responseBuilder.append(line).append("\n");
            }
        } catch (IOException e) {
            logger.error("http get error", e);
            throw e;
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    logger.error("close reader error", e);
                    throw e;
                }
            }
        }

        return responseBuilder.toString();
    }

}
