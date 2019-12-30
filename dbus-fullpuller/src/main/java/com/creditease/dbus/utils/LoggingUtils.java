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


package com.creditease.dbus.utils;

import org.slf4j.Logger;

import java.sql.SQLException;

/**
 * A helper class for logging.
 */
public final class LoggingUtils {

    private LoggingUtils() {
    }

    /**
     * Log entire chain of SQLExceptions using old SQLException.getNextException
     * interface instead of new Throwable.getCause().
     */
    public static void logAll(Logger log, SQLException e) {
        logAll(log, null, e);
    }

    public static void logAll(Logger log, String message, SQLException e) {
        log.error(message == null ? "Top level exception: " : message, e);
        e = e.getNextException();
        int indx = 1;
        while (e != null) {
            log.error("Chained exception " + indx + ": ", e);
            e = e.getNextException();
            indx++;
        }
    }
}

