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


package com.creditease.dbus.canal.utils;

import com.creditease.dbus.canal.bean.DeployPropsBean;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

/**
 * User: 王少楠
 * Date: 2018-08-07
 * Desc:
 */
public class FileUtils {

    public static BufferedWriter bw = null;
    public static boolean inited = false;

    public static void init(BufferedWriter bufferedWriter) {
        bw = bufferedWriter;
        inited = true;
    }

    public static DeployPropsBean readProps(String path) throws Exception {
        try (InputStream ins = new BufferedInputStream(new FileInputStream(path))) {
            Properties deployProps = new Properties();
            deployProps.load(ins);
            DeployPropsBean props = new DeployPropsBean();
            props.setDsName(deployProps.getProperty("dsname").trim());
            props.setZkPath(deployProps.getProperty("zk.path").trim());
            props.setSlavePath(deployProps.getProperty("canal.address").trim());
            props.setCanalUser(deployProps.getProperty("canal.user").trim());
            props.setCanalPwd(deployProps.getProperty("canal.pwd").trim());
            String slaveId = deployProps.getProperty("canal.slaveId");
            slaveId = StringUtils.isBlank(slaveId) ? "1234" : slaveId;
            props.setCanalSlaveId(slaveId.trim());
            String bootstrapServers = deployProps.getProperty("bootstrap.servers");
            props.setBootstrapServers(bootstrapServers);

            ins.close();
            return props;
        } catch (Exception e) {
            throw e;
        }
    }

    public static String getValueFromFile(String path, String key) throws Exception {
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.matches(key + "\\s*=.*")) {
                    return StringUtils.trim(StringUtils.split(line, "=")[1]);
                }
            }
            return null;
        } catch (Exception e) {
            throw e;
        }
    }

    /**
     * 更新properties文件某key的value
     */
    public static void writeProperties(String filePath, String key, String value) throws Exception {
        try {
            Boolean over = false;
            File props = new File(filePath);
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(props)));

            List<String> fileContent = new LinkedList();
            while (true) {
                String line = br.readLine();
                if (line == null)
                    break;
                if (!over && line.contains(key)) {
                    fileContent.add(value);
                    over = true;
                } else {
                    fileContent.add(line);
                }
            }

            br.close();

            PrintWriter pw = new PrintWriter(props);
            for (String line : fileContent) {
                pw.println(line);
            }
            pw.close();
            writeAndPrint("props: " + value);

        } catch (Exception e) {
            writeAndPrint(" write props error:  file: " + filePath + "properties:" + value);
            throw e;
        }
    }

    public static void writeAndPrint(String log) throws Exception {
        if (bw != null) {
            bw.write(log + "\n");
        }
        System.out.println(log);
    }

}
