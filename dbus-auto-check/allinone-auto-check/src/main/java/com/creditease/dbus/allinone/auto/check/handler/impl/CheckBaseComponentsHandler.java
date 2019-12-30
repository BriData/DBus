/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2019 Bridata
 * ==
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * >>
 */


package com.creditease.dbus.allinone.auto.check.handler.impl;

import com.creditease.dbus.allinone.auto.check.handler.AbstractHandler;

import java.io.BufferedWriter;

/**
 * Created by Administrator on 2018/8/1.
 */
public class CheckBaseComponentsHandler extends AbstractHandler {

    @Override
    public void check(BufferedWriter bw) throws Exception {
        checkZk(bw);
        checkKafka(bw);
        checkStrom(bw);
        checkInfluxdb(bw);
        checkGrafana(bw);
        checkHb(bw);
        checkLogstash(bw);
    }

    public void checkZk(BufferedWriter bw) throws Exception {
        bw.newLine();
        bw.write("check base component zookeeper start: ");
        bw.newLine();
        bw.write("============================================");
        bw.newLine();
        String[] cmd = {"/bin/sh", "-c", "jps -l | grep QuorumPeerMain"};
        Process process = Runtime.getRuntime().exec(cmd);
        Thread outThread = new Thread(new StreamRunnable(process.getInputStream(), bw));
        Thread errThread = new Thread(new StreamRunnable(process.getErrorStream(), bw));
        outThread.start();
        errThread.start();
        int exitValue = process.waitFor();
        if (exitValue != 0) process.destroyForcibly();
    }

    public void checkKafka(BufferedWriter bw) throws Exception {
        bw.newLine();
        bw.write("check base component kafka start: ");
        bw.newLine();
        bw.write("============================================");
        bw.newLine();
        String[] cmd = {"/bin/sh", "-c", "jps -l | grep Kafka"};
        Process process = Runtime.getRuntime().exec(cmd);
        Thread outThread = new Thread(new StreamRunnable(process.getInputStream(), bw));
        Thread errThread = new Thread(new StreamRunnable(process.getErrorStream(), bw));
        outThread.start();
        errThread.start();
        int exitValue = process.waitFor();
        if (exitValue != 0) process.destroyForcibly();
    }

    public void checkStrom(BufferedWriter bw) throws Exception {
        bw.newLine();
        bw.write("check base component storm start: ");
        bw.newLine();
        bw.write("============================================");
        bw.newLine();
        String[] cmd = {"/bin/sh", "-c", "jps -l | grep storm"};
        Process process = Runtime.getRuntime().exec(cmd);
        Thread outThread = new Thread(new StreamRunnable(process.getInputStream(), bw));
        Thread errThread = new Thread(new StreamRunnable(process.getErrorStream(), bw));
        outThread.start();
        errThread.start();
        int exitValue = process.waitFor();
        if (exitValue != 0) process.destroyForcibly();
    }

    public void checkInfluxdb(BufferedWriter bw) throws Exception {
        bw.newLine();
        bw.write("check base component influxdb start: ");
        bw.newLine();
        bw.write("============================================");
        bw.newLine();
        String[] cmd = {"/bin/sh", "-c", "ps -ef | grep influxdb"};
        Process process = Runtime.getRuntime().exec(cmd);
        Thread outThread = new Thread(new StreamRunnable(process.getInputStream(), bw));
        Thread errThread = new Thread(new StreamRunnable(process.getErrorStream(), bw));
        outThread.start();
        errThread.start();
        int exitValue = process.waitFor();
        if (exitValue != 0) process.destroyForcibly();
    }

    public void checkGrafana(BufferedWriter bw) throws Exception {
        bw.newLine();
        bw.write("check base component grafana start: ");
        bw.newLine();
        bw.write("============================================");
        bw.newLine();
        String[] cmd = {"/bin/sh", "-c", "ps -ef | grep grafana"};
        Process process = Runtime.getRuntime().exec(cmd);
        Thread outThread = new Thread(new StreamRunnable(process.getInputStream(), bw));
        Thread errThread = new Thread(new StreamRunnable(process.getErrorStream(), bw));
        outThread.start();
        errThread.start();
        int exitValue = process.waitFor();
        if (exitValue != 0) process.destroyForcibly();
    }

    public void checkHb(BufferedWriter bw) throws Exception {
        bw.newLine();
        bw.write("check base component heartbeat start: ");
        bw.newLine();
        bw.write("============================================");
        bw.newLine();
        String[] cmd = {"/bin/sh", "-c", "jps -l | grep heartbeat.start"};
        Process process = Runtime.getRuntime().exec(cmd);
        Thread outThread = new Thread(new StreamRunnable(process.getInputStream(), bw));
        Thread errThread = new Thread(new StreamRunnable(process.getErrorStream(), bw));
        outThread.start();
        errThread.start();
        int exitValue = process.waitFor();
        if (exitValue != 0) process.destroyForcibly();
    }

    public void checkLogstash(BufferedWriter bw) throws Exception {
        bw.newLine();
        bw.write("check base component logstash start: ");
        bw.newLine();
        bw.write("============================================");
        bw.newLine();
        String[] cmd = {"/bin/sh", "-c", "jps -l | grep jruby.Main"};
        Process process = Runtime.getRuntime().exec(cmd);
        Thread outThread = new Thread(new StreamRunnable(process.getInputStream(), bw));
        Thread errThread = new Thread(new StreamRunnable(process.getErrorStream(), bw));
        outThread.start();
        errThread.start();
        int exitValue = process.waitFor();
        if (exitValue != 0) process.destroyForcibly();
    }

}
