package com.creditease.dbus.canal.auto;


import com.creditease.dbus.canal.utils.CanalUtils;
import org.apache.commons.cli.*;

/**
 * This is Description
 *
 * @author xiancangao
 * @date 2018/12/13
 */
public class DeleteLine {
    public static String dsName = null;
    public static String canalName = null;

    public static void main(String[] args) {
        try {
            parseCommandArgs(args);
            autoDeploy();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void autoDeploy() throws Exception {
        CanalUtils.exec("./" + canalName + "/bin/stop.sh");
        CanalUtils.exec("rm -r " + canalName);
        CanalUtils.exec("rm " + dsName + ".log");
    }

    private static void parseCommandArgs(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("dn", "dsName", true, "");
        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine line = parser.parse(options, args);
            if (line.hasOption("dsName")) {
                dsName = line.getOptionValue("dsName");
            }
            canalName = "canal-" + dsName;
        } catch (ParseException e) {
            throw e;
        }
    }
}
