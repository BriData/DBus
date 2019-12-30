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


package com.creditease.dbus.auto.utils;

import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

public class CmdUtils {

    public static String executeNormalCmd(String cmd) throws Exception {
        return executeNormalCmd(new String[]{"sh", "-c", cmd});
    }

    private static String executeNormalCmd(String... cmd) throws Exception {
        Process ps = Runtime.getRuntime().exec(cmd);
        BufferedReader br = null;
        BufferedReader bre = null;
        try {
            br = new BufferedReader(new InputStreamReader(ps.getInputStream()));
            String result = br.lines().parallel().collect(Collectors.joining(System.getProperty("line.separator")));
            bre = new BufferedReader(new InputStreamReader(ps.getErrorStream()));
            String err = bre.lines().parallel().collect(Collectors.joining(System.getProperty("line.separator")));
            if (StringUtils.isNotBlank(err)) {
                System.out.println(err);
            }
            ps.waitFor();
            return result;
        } finally {
            if (br != null) {
                br.close();
            }
            if (bre != null) {
                bre.close();
            }
        }
    }
}
