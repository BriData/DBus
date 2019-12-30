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


package com.creditease.dbus.allinone.auto.check.handler;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public abstract class AbstractHandler implements IHandler {

    private static Logger logger = LoggerFactory.getLogger(AbstractHandler.class);

    @Override
    public boolean process(BufferedWriter bw) {
        boolean isOk = true;
        try {
            check(bw);
        } catch (Exception e) {
            logger.error("process error", e);
            isOk = false;
        }
        return isOk;
    }

    public abstract void check(BufferedWriter bw) throws Exception;

    protected class StreamRunnable implements Runnable {

        private Reader reader = null;

        private BufferedReader br = null;

        private BufferedWriter bw = null;

        private String filter = "";

        public StreamRunnable(InputStream is, BufferedWriter bw) {
            this(is, bw, "");
        }

        public StreamRunnable(InputStream is, BufferedWriter bw, String filter) {
            Reader reader = new InputStreamReader(is);
            br = new BufferedReader(reader);
            this.bw = bw;
            this.filter = filter;
        }

        @Override
        public void run() {
            try {
                String line = br.readLine();
                while (StringUtils.isNotBlank(line)) {
                    if (StringUtils.isNotBlank(filter)) {
                        if (StringUtils.contains(line, filter)) {
                            bw.write(line);
                            bw.newLine();
                        }
                    } else {
                        bw.write(line);
                        bw.newLine();
                    }
                    line = br.readLine();
                }
            } catch (Exception e) {
                logger.error("stream runnable error", e);
            } finally {
                close(br);
                close(reader);
            }
        }

        private void close(Closeable closeable) {
            try {
                if (closeable != null)
                    closeable.close();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

}
