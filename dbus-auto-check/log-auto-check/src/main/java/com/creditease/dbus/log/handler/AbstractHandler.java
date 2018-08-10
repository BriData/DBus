/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2017 Bridata
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

package com.creditease.dbus.log.handler;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public abstract class AbstractHandler implements IHandler {

    private static Logger logger = LoggerFactory.getLogger(AbstractHandler.class);

    @Override
    public boolean processCheck(BufferedWriter bw) {
        boolean isOk = true;
        try {
            check(bw);
        } catch (Exception e) {
            logger.error("processCheck error", e);
            isOk = false;
        }
        return isOk;
    }

    @Override
    public boolean processDeploy(BufferedWriter bw) {
        boolean isOk = true;
        try {
            deploy(bw);
        } catch (Exception e) {
            logger.error("processDeploy error", e);
            isOk = false;
        }
        return isOk;
    }


    public abstract void check(BufferedWriter bw) throws Exception;

    public abstract void deploy(BufferedWriter bw) throws Exception;

}
