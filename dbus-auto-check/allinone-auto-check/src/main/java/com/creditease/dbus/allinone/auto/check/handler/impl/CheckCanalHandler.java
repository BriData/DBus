package com.creditease.dbus.allinone.auto.check.handler.impl;

import java.io.BufferedWriter;

import com.creditease.dbus.allinone.auto.check.bean.AutoCheckConfigBean;
import com.creditease.dbus.allinone.auto.check.container.AutoCheckConfigContainer;
import com.creditease.dbus.allinone.auto.check.handler.AbstractHandler;
import com.creditease.dbus.allinone.auto.check.utils.MsgUtil;
import com.creditease.dbus.commons.IZkService;
import com.creditease.dbus.commons.ZkService;

/**
 * Created by Administrator on 2018/8/1.
 */
public class CheckCanalHandler extends AbstractHandler {

    @Override
    public void check(BufferedWriter bw) throws Exception {
        checkCanalZkNode(bw);
        checkCanalStart(bw);
    }

    private void checkCanalZkNode(BufferedWriter bw) throws Exception {
        bw.newLine();
        bw.write("check canal start: ");
        bw.newLine();
        bw.write("============================================");
        AutoCheckConfigBean conf = AutoCheckConfigContainer.getInstance().getAutoCheckConf();
        IZkService zkService = new ZkService(conf.getZkHost(), 5000);
        if (zkService.isExists(conf.getCanalZkNode())) {
            bw.newLine();
            bw.write(MsgUtil.format("zk path [{0}] exists.", conf.getCanalZkNode()));
            bw.newLine();
        } else {
            zkService.createNode(conf.getCanalZkNode(), new byte[0]);
            bw.write(MsgUtil.format("create zk path [{0}] success.", conf.getCanalZkNode()));
            bw.newLine();
        }
    }

    private void checkCanalStart(BufferedWriter bw) throws Exception {
        String[] cmd = { "/bin/sh", "-c", "jps -l | grep CanalLauncher" };
        Process process = Runtime.getRuntime().exec(cmd);
        Thread outThread = new Thread(new StreamRunnable(process.getInputStream(), bw));
        Thread errThread = new Thread(new StreamRunnable(process.getErrorStream(), bw));
        outThread.start();
        errThread.start();
        int exitValue = process.waitFor();
        if (exitValue != 0) process.destroyForcibly();
    }

}
