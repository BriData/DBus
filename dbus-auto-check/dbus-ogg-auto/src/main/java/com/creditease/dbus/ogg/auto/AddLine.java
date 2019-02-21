package com.creditease.dbus.ogg.auto;

import com.creditease.dbus.ogg.utils.FileUtil;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.util.*;

/**
 * This is Description
 *
 * @author xiancangao
 * @date 2018/12/12
 */
public class AddLine {
    public static String type = null;
    public static String dsName = null;
    public static String schemaName = null;
    public static String tableNames = null;
    public static String dirprmPath = null;
    public static String NLS_LANG = null;
    public static String replicatName = null;
    public static String userDir = System.getProperty("user.dir");
    public static String paramFilePath = null;
    public static String proFilePath = null;
    public static String kafkaProducerPath = null;
    public static String bootstrapServers = null;

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
                break;
            case "newSchema":
                newSchema();
                break;
            case "newTable":
                newTable();
                break;
        }
    }

    private static void newLine() throws Exception {
        HashMap<String, String> updateProps = new HashMap<>();
        //如果kafka_producer.properties不存在新增
        updateProps.put("bootstrap.servers", bootstrapServers);
        FileUtil.WriteProperties(userDir + "/conf/kafka_producer.properties", updateProps, kafkaProducerPath);
        //新增replicate.props文件
        updateProps.clear();
        updateProps.put("gg.handler.kafkahandler.topicMappingTemplate", dsName);
        updateProps.put("gg.handler.kafkahandler.schemaTopicName", dsName + "_schema");
        FileUtil.WriteProperties(userDir + "/conf/replicate.properties", updateProps, proFilePath);
        //新增param.prm文件
        newParamFile();
        if (StringUtils.isNotBlank(schemaName)) {
            addSchemaToParamFile();
            System.out.println("add schema." + schemaName);
        }
        if (StringUtils.isNotBlank(tableNames)) {
            System.out.println("add tables." + tableNames);
            addTableToParamFile();
        }
    }

    private static void newSchema() throws Exception {
        if (StringUtils.isNotBlank(schemaName)) {
            System.out.println("add schema." + schemaName);
            addSchemaToParamFile();
        }
        if (StringUtils.isNotBlank(tableNames)) {
            System.out.println("add tables." + tableNames);
            addTableToParamFile();
        }
    }

    private static void newTable() throws Exception {
        if (StringUtils.isNotBlank(tableNames)) {
            addTableToParamFile();
        }
    }

    private static void newParamFile() throws Exception {
        BufferedWriter bw = null;
        try {
            File paramFile = new File(paramFilePath);
            if (!paramFile.exists()) {
                paramFile.createNewFile();
            }
            //!!!!!!!存在的话会直接覆盖原有的文件,这点请注意
            StringBuilder sb = new StringBuilder();
            sb.append("REPLICAT ").append(replicatName).append("\n");
            sb.append("SETENV (NLS_LANG = ").append(NLS_LANG).append(")\n");
            sb.append("TARGETDB LIBFILE libggjava.so SET property=dirprm/").append(replicatName).append(".props\n");
            sb.append("DDL INCLUDE ALL\n");
            sb.append("GROUPTRANSOPS 500\n");
            sb.append("MAXTRANSOPS 1000\n\n");
            sb.append("MAP DBUS.TEST_TABLE, TARGET DBUS.TEST_TABLE;\n");
            sb.append("MAP DBUS.DB_FULL_PULL_REQUESTS, TARGET DBUS.DB_FULL_PULL_REQUESTS, WHERE (SCHEMA_NAME = 'DBUS');\n");
            sb.append("MAP DBUS.DB_HEARTBEAT_MONITOR, TARGET DBUS.DB_HEARTBEAT_MONITOR, WHERE(SCHEMA_NAME = 'DBUS');\n");
            sb.append("MAP DBUS.META_SYNC_EVENT, TARGET DBUS.META_SYNC_EVENT, WHERE (TABLE_OWNER = 'DBUS');\n\n");
            bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(paramFile)));
            bw.write(sb.toString());
            bw.flush();
        } catch (Exception e) {
            System.out.println("Exception when new file " + paramFilePath);
            throw e;
        } finally {
            if (bw != null) {
                bw.flush();
                bw.close();
            }
        }
    }

    private static void addSchemaToParamFile() throws Exception {
        BufferedWriter bw = null;
        BufferedReader br = null;
        try {
            br = new BufferedReader(new InputStreamReader(new FileInputStream(paramFilePath)));
            String line = null;
            ArrayList<String> input = new ArrayList<>();
            while ((line = br.readLine()) != null) {
                input.add(line);
            }
            if (br != null) {
                br.close();
            }
            ArrayList<String> output = new ArrayList<>(input);
            for (int i = 0; i < input.size(); i++) {
                boolean flag = false;
                String s = input.get(i);
                if (s.contains("DBUS.DB_FULL_PULL_REQUESTS")) {
                    flag = true;
                }
                if (s.contains(schemaName)) {
                    break;
                }
                if (flag && s.contains(";")) {
                    s = s.substring(0, s.lastIndexOf(")"));
                    s += " OR SCHEMA_NAME = '" + schemaName + "');";
                    output.set(i, s);
                    break;
                }
            }
            for (int i = 0; i < input.size(); i++) {
                boolean flag = false;
                String s = input.get(i);
                if (s.contains("DBUS.DB_HEARTBEAT_MONITOR")) {
                    flag = true;
                }
                if (s.contains(schemaName)) {
                    break;
                }
                if (flag && s.contains(";")) {
                    s = s.substring(0, s.lastIndexOf(")"));
                    s += " OR SCHEMA_NAME = '" + schemaName + "');";
                    output.set(i, s);
                    break;
                }
            }
            for (int i = 0; i < input.size(); i++) {
                boolean flag = false;
                String s = input.get(i);
                if (s.contains("DBUS.META_SYNC_EVENT")) {
                    flag = true;
                }
                if (s.contains(schemaName)) {
                    break;
                }
                if (flag && s.contains(";")) {
                    s = s.substring(0, s.lastIndexOf(")"));
                    s += " OR TABLE_OWNER = '" + schemaName + "');";
                    output.set(i, s);
                    break;
                }
            }
            bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(paramFilePath)));
            for (String s : output) {
                bw.write(s + "\n");
            }
            bw.flush();
        } catch (Exception e) {
            System.out.println("Exception when add schema to file " + paramFilePath);
            throw e;
        } finally {
            if (bw != null) {
                bw.flush();
                bw.close();
            }
            if (br != null) {
                br.close();
            }
        }
    }

    private static void addTableToParamFile() throws Exception {
        BufferedWriter bw = null;
        BufferedReader br = null;
        try {
            br = new BufferedReader(new InputStreamReader(new FileInputStream(paramFilePath)));
            String line = null;
            ArrayList<String> input = new ArrayList<>();
            while ((line = br.readLine()) != null) {
                input.add(line);
            }
            if (br != null) {
                br.close();
            }
            List<String> names = Arrays.asList(tableNames.split(","));
            ArrayList<String> addNames = new ArrayList<>();
            for (String name : names) {
                addNames.add(name);
            }
            for (String name : names) {
                for (String lineString : input) {
                    //存在不添加
                    if (lineString.contains("MAP " + schemaName + "." + name) && lineString.contains("TARGET " + schemaName + "." + name)) {
                        System.out.println("remove table " + name);
                        addNames.remove(name);
                    }
                }
            }
            bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(paramFilePath, true)));
            if (addNames.size() > 0) {
                bw.write("--" + new Date() + "\n");
            }
            for (String name : addNames) {
                bw.write("MAP " + schemaName + "." + name + ", TARGET " + schemaName + "." + name + ";\n");
                System.out.println("add table " + name);
            }
        } catch (Exception e) {
            System.out.println("Exception when add table to file " + paramFilePath);
            throw e;
        } finally {
            if (bw != null) {
                bw.flush();
                bw.close();
            }
            if (br != null) {
                br.close();
            }
        }
    }

    private static void parseCommandArgs(String[] args) throws Exception {
        System.out.println(Arrays.asList(args));
        Options options = new Options();

        options.addOption("t", "type", true, "newLine,newSchema,newTable");
        options.addOption("dn", "dsName", true, "");
        options.addOption("sn", "schemaName", true, "");
        options.addOption("tn", "tableNames", true, "");
        options.addOption("dp", "dirprmPath", true, "");
        options.addOption("nl", "NLS_LANG", true, "");
        options.addOption("rn", "replicatName", true, "");
        options.addOption("bs", "bootstrap.servers", true, "");

        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine line = parser.parse(options, args);
            if (line.hasOption("type")) {
                type = line.getOptionValue("type");
            }
            if (line.hasOption("dsName")) {
                dsName = line.getOptionValue("dsName");
            }
            if (line.hasOption("schemaName")) {
                schemaName = line.getOptionValue("schemaName");
            }
            if (line.hasOption("tableNames")) {
                tableNames = line.getOptionValue("tableNames");
            }
            if (line.hasOption("dirprmPath")) {
                dirprmPath = line.getOptionValue("dirprmPath");
            }
            if (line.hasOption("NLS_LANG")) {
                NLS_LANG = line.getOptionValue("NLS_LANG");
            } else {
                NLS_LANG = "AMERICAN_AMERICA.AL32UTF8";
            }
            if (line.hasOption("replicatName")) {
                replicatName = line.getOptionValue("replicatName");
            } else {
                replicatName = dsName;
            }
            paramFilePath = dirprmPath + "/" + replicatName.toLowerCase() + ".prm";
            proFilePath = dirprmPath + "/" + replicatName.toLowerCase() + ".props";
            kafkaProducerPath = dirprmPath + "/kafka_producer.properties";
            if (line.hasOption("bootstrap.servers")) {
                bootstrapServers = line.getOptionValue("bootstrap.servers");
            }
            System.out.println("type:" + type);
            System.out.println("dsName:" + dsName);
            System.out.println("schemaName:" + schemaName);
            System.out.println("tableNames:" + tableNames);
            System.out.println("dirprmPath:" + dirprmPath);
            System.out.println("NLS_LANG:" + NLS_LANG);
            System.out.println("replicatName:" + replicatName);
            System.out.println("paramFilePath:" + paramFilePath);
            System.out.println("proFilePath:" + proFilePath);
            System.out.println("kafkaProducerPath:" + kafkaProducerPath);
            System.out.println("bootstrapServers:" + bootstrapServers);

        } catch (ParseException e) {
            throw e;
        }
    }
}
