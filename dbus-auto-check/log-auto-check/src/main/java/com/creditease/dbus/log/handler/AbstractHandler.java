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


package com.creditease.dbus.log.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;

public abstract class AbstractHandler implements IHandler {

    private static Logger logger = LoggerFactory.getLogger(AbstractHandler.class);

    @Override
    public boolean processCheckDeploy(BufferedWriter bw) {
        boolean isOk = true;
        try {
            checkDeploy(bw);
        } catch (Exception e) {
            logger.error("processCheckDeploy error", e);
            isOk = false;
        }
        return isOk;
    }

    public abstract void checkDeploy(BufferedWriter bw) throws Exception;

}
