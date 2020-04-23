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


package com.creditease.dbus.canal.auto;


import com.creditease.dbus.canal.utils.CanalUtils;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.creditease.dbus.canal.utils.FileUtils.getValueFromFile;
import static com.creditease.dbus.canal.utils.FileUtils.writeProperties;

/**
 * This is Description
 *
 * @author xiancangao
 * @date 2018/12/12
 */
public class AddLine {
    public static String type = "newLine";
    public static String dsName = null;
    public static String zkString = null;
    public static String address = null;
    public static String user = null;
    public static String pass = null;
    public static Integer slaveId = null;
    public static String bootstrapServers = null;
    public static String tableNames = null;
    public static String userDir = System.getProperty("user.dir");
    public static String DEFAULT_FILTER = ".*\\\\..*";

    public static void main(String[] args) {
        try {
            parseCommandArgs(args);
            autoDeploy();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void autoDeploy() throws Exception {
        switch (type) {
            case "newLine":
                newLine();
                AutoDeployStart.main(null);
                break;
            case "editFilter":
                editFilter();
                break;
            case "initFilter":
                initFilter();
                break;
            case "deleteFilter":
                deleteFilter();
                break;
        }
    }

    private static void deleteFilter() throws Exception {
        if (StringUtils.isNotBlank(tableNames)) {
            System.out.println("delete filter." + tableNames);
            deleteTableFromParamFile();
        }
        restart();
    }

    private static void initFilter() throws Exception {
        if (StringUtils.isNotBlank(tableNames)) {
            System.out.println("init filter." + tableNames);
            addAllTableToParamFile();
        }
        restart();
    }

    private static void editFilter() throws Exception {
        if (StringUtils.isNotBlank(tableNames)) {
            System.out.println("edit filter." + tableNames);
            addTableToParamFile();
        }
        restart();
    }

    private static void addAllTableToParamFile() throws Exception {
        String userdir = System.getProperty("user.dir");
        String paramFilePath = String.format("%s/canal-%s/conf/%s/instance.properties", userdir, dsName, dsName);
        if (StringUtils.isBlank(tableNames)) {
            tableNames = DEFAULT_FILTER;
        }
        writeProperties(paramFilePath, "canal.instance.filter.regex", "canal.instance.filter.regex=" + tableNames);
    }

    private static void deleteTableFromParamFile() throws Exception {
        String userdir = System.getProperty("user.dir");
        String paramFilePath = String.format("%s/canal-%s/conf/%s/instance.properties", userdir, dsName, dsName);
        String filterRegex = getValueFromFile(paramFilePath, "canal.instance.filter.regex");
        if (StringUtils.equals(DEFAULT_FILTER, filterRegex)) {
            return;
        }
        List<String> oldFilterList = Arrays.asList(StringUtils.split(filterRegex, ","));
        // 删除的表
        List<String> tableNameList = Arrays.asList(StringUtils.split(tableNames, ","));
        // 新的filter表
        List<String> newFilterList = new ArrayList<>();
        for (String tableName : oldFilterList) {
            if (!tableNameList.contains(tableName)) {
                newFilterList.add(tableName);
            }
        }
        String newfilterRegex = newFilterList.stream().collect(Collectors.joining(","));
        if (StringUtils.isBlank(newfilterRegex)) {
            newfilterRegex = DEFAULT_FILTER;
        }
        writeProperties(paramFilePath, "canal.instance.filter.regex", "canal.instance.filter.regex=" + newfilterRegex);
    }

    private static void restart() throws Exception {
        String canalPath = String.format("%s/canal-%s", System.getProperty("user.dir"), dsName);
        CanalUtils.start(canalPath);
    }

    private static void addTableToParamFile() throws Exception {
        String userdir = System.getProperty("user.dir");
        String paramFilePath = String.format("%s/canal-%s/conf/%s/instance.properties", userdir, dsName, dsName);
        String filterRegex = getValueFromFile(paramFilePath, "canal.instance.filter.regex");
        if (StringUtils.equals(DEFAULT_FILTER, filterRegex)) {
            filterRegex = "";
        }
        List<String> oldFilterList = Arrays.asList(StringUtils.split(filterRegex, ","));
        // 新添加的表
        List<String> tableNameList = Arrays.asList(StringUtils.split(tableNames, ","));
        // 新的filter表
        List<String> newFilterList = new ArrayList<>();
        for (String tableName : tableNameList) {
            if (!oldFilterList.contains(tableName)) {
                newFilterList.add(tableName);
            }
        }
        newFilterList.addAll(oldFilterList);
        String newfilterRegex = newFilterList.stream().collect(Collectors.joining(","));
        if (StringUtils.isBlank(newfilterRegex)) {
            newfilterRegex = ".*\\\\..*";
        }
        writeProperties(paramFilePath, "canal.instance.filter.regex", "canal.instance.filter.regex=" + newfilterRegex);
    }

    private static void newLine() throws Exception {
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(userDir + "/conf/canal-auto.properties")));
            bw.write("#数据源名称，需要与dbus keeper中添加的一致\n");
            bw.write("dsname=" + dsName + "\n");
            bw.write("zk地址,替换成自己的信息\n");
            bw.write("zk.path=" + zkString + "\n");
            bw.write("#canal 用户连接地址。即：要canal去同步的源端库的备库的地址\n");
            bw.write("canal.address=" + address + "\n");
            bw.write("#canal用户名\n");
            bw.write("canal.user=" + user + "\n");
            bw.write("#canal密码，替换成自己配置的\n");
            bw.write("canal.pwd=" + pass + "\n");
            bw.write("#canal slave id\n");
            bw.write("canal.slaveId=" + slaveId + "\n");
            bw.write("#bootstrap.servers\n");
            bw.write("bootstrap.servers=" + bootstrapServers);
            bw.flush();
        } catch (Exception e) {
            System.out.println("Exception when edit file :" + userDir + "/conf/canal-auto.properties");
            throw e;
        } finally {
            if (bw != null) {
                bw.flush();
                bw.close();
            }
        }

    }

    private static void parseCommandArgs(String[] args) throws Exception {
        Options options = new Options();

        options.addOption("t", "type", true, "newSchema");
        options.addOption("dn", "dsName", true, "");
        options.addOption("zk", "zkString", true, "");
        options.addOption("a", "address", true, "");
        options.addOption("u", "user", true, "");
        options.addOption("p", "pass", true, "");
        options.addOption("s", "slaveId", true, "");
        options.addOption("bs", "bootstrap.servers", true, "");
        options.addOption("tn", "tableNames", true, "");


        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine line = parser.parse(options, args);
            if (line.hasOption("type")) {
                type = line.getOptionValue("type");
            }
            if (line.hasOption("dsName")) {
                dsName = line.getOptionValue("dsName");
            }
            if (line.hasOption("zkString")) {
                zkString = line.getOptionValue("zkString");
            }
            if (line.hasOption("address")) {
                address = line.getOptionValue("address");
            }
            if (line.hasOption("user")) {
                user = line.getOptionValue("user");
            }
            if (line.hasOption("pass")) {
                pass = line.getOptionValue("pass");
            }
            if (line.hasOption("slaveId")) {
                slaveId = Integer.parseInt(line.getOptionValue("slaveId"));
            }
            if (line.hasOption("bootstrap.servers")) {
                bootstrapServers = line.getOptionValue("bootstrap.servers");
            }
            if (line.hasOption("tableNames")) {
                tableNames = line.getOptionValue("tableNames");
            }

        } catch (ParseException e) {
            throw e;
        }
    }
}
