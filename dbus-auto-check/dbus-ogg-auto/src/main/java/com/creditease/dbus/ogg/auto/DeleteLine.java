package com.creditease.dbus.ogg.auto;

import org.apache.commons.cli.*;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This is Description
 *
 * @author xiancangao
 * @date 2018/12/13
 */
public class DeleteLine {
    public static String type = null;
    public static String dsName = null;
    public static String schemaName = null;
    public static String tableNames = null;
    public static String dirprmPath = null;
    public static String replicatName = null;
    public static String paramFilePath = null;
    public static String proFilePath = null;

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
            case "delLine":
                delLine();
                break;
            case "delSchema":
                delSchema();
                break;
            case "delTable":
                delTable();
                break;
        }
    }

    private static void delLine() throws Exception {
        File prmFile = new File(paramFilePath);
        if (prmFile.exists()) {
            prmFile.delete();
        }
        File proFile = new File(proFilePath);
        if (proFile.exists()) {
            proFile.delete();
        }
    }

    private static void delSchema() throws Exception {
        if (StringUtils.isNotBlank(schemaName)) {
            delSchemaFromParamFile();
        }
    }

    private static void delTable() throws Exception {
        if (StringUtils.isNotBlank(tableNames)) {
            delTableFromParamFile();
        }
    }

    private static void delSchemaFromParamFile() throws Exception {
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
            for (String lineString : input) {
                if (lineString.contains("MAP " + schemaName + ".") && lineString.contains("TARGET " + schemaName + ".")) {
                    output.remove(lineString);
                }
            }
            bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(paramFilePath)));
            for (String s : output) {
                bw.write(s + "\n");
            }
            bw.flush();
        } catch (Exception e) {
            System.out.println("Exception when del schema to file " + paramFilePath);
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

    private static void delTableFromParamFile() throws Exception {
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
            List<String> tables = Arrays.asList(tableNames.split(","));
            ArrayList<String> output = new ArrayList<>(input);
            for (String lineString : input) {
                for (String name : tables) {
                    if (lineString.contains(schemaName + "." + name)) {
                        output.remove(lineString);
                    }
                }
            }
            bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(paramFilePath)));
            for (String s : output) {
                bw.write(s + "\n");
            }
            bw.flush();
        } catch (Exception e) {
            System.out.println("Exception when del schema to file " + paramFilePath);
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
        Options options = new Options();

        options.addOption("t", "type", true, "delLine,delSchema,delTable");
        options.addOption("dn", "dsName", true, "");
        options.addOption("sn", "schemaName", true, "");
        options.addOption("tn", "tableNames", true, "");
        options.addOption("dp", "dirprmPath", true, "");
        options.addOption("rn", "replicatName", true, "");

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
            if (line.hasOption("replicatName")) {
                replicatName = line.getOptionValue("replicatName");
            } else {
                replicatName = dsName;
            }
            paramFilePath = dirprmPath + "/" + replicatName + ".prm";
            proFilePath = dirprmPath + "/" + replicatName + ".props";
        } catch (ParseException e) {
            throw e;
        }
    }
}
