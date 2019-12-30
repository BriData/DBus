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


package com.creditease.dbus.util;

import java.io.*;

public class TranscodeUtils {

    public static void transcode(File in, File out, String inCode, String outCode) throws Exception {
        InputStreamReader isr = null;
        OutputStreamWriter osw = null;
        try {
            isr = new InputStreamReader(new FileInputStream(in), inCode);
            osw = new OutputStreamWriter(new FileOutputStream(out), outCode);

            char[] chr = new char[1024];
            int length;
            while ((length = isr.read(chr, 0, 1024)) > 0) {
                osw.write(chr, 0, length);
                osw.flush();
            }
        } finally {
            if (isr != null) {
                isr.close();
            }
            if (osw != null) {
                osw.close();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        //File in = new File("D:\\temp\\test.csv");
        //File out = new File("D:\\temp\\test-gbk.csv");
        //transcode(in,out,"utf-8","gbk");

        File out = new File("D:\\temp\\test-utf8.csv");
        File in = new File("D:\\temp\\test-gbk.csv");
        transcode(in, out, "gbk", "utf-8");
    }

}
