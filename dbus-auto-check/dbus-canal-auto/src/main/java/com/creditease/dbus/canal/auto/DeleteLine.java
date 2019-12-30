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
