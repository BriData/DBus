/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2018 Bridata
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

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.creditease.dbus.common.utils;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods to format and print ResultSet objects.
 */
public class ResultSetPrinter {

    private Logger LOG = LoggerFactory.getLogger(getClass());

  // max output width to allocate to any column of the printed results.
  private static final int MAX_COL_WIDTH = 20;

  /**
   * Print 'str' to the string builder, padded to 'width' chars.
   */
  private static void printPadded(StringBuilder sb, String str, int width) {
    int numPad;
    if (null == str) {
      sb.append("(null)");
      numPad = width - "(null)".length();
    } else {
      sb.append(str);
      numPad = width - str.length();
    }

    for (int i = 0; i < numPad; i++) {
      sb.append(' ');
    }
  }

  private static final String COL_SEPARATOR = " | ";
  private static final String LEFT_BORDER = "| ";

  /**
   * Format the contents of the ResultSet into something that could be printed
   * neatly; the results are appended to the supplied StringBuilder.
   */
  public final void printResultSet(PrintWriter pw, ResultSet results)
      throws IOException {
    try {
      StringBuilder sbNames = new StringBuilder();
      int cols = results.getMetaData().getColumnCount();

      int [] colWidths = new int[cols];
      ResultSetMetaData metadata = results.getMetaData();
      sbNames.append(LEFT_BORDER);
      for (int i = 1; i < cols + 1; i++) {
        String colName = metadata.getColumnLabel(i);
        colWidths[i - 1] = Math.min(metadata.getColumnDisplaySize(i),
            MAX_COL_WIDTH);
        if (colName == null || colName.equals("")) {
          colName = metadata.getColumnName(i) + "*";
        }
        printPadded(sbNames, colName, colWidths[i - 1]);
        sbNames.append(COL_SEPARATOR);
      }
      sbNames.append('\n');

      StringBuilder sbPad = new StringBuilder();
      for (int i = 0; i < cols; i++) {
        for (int j = 0; j < COL_SEPARATOR.length() + colWidths[i]; j++) {
          sbPad.append('-');
        }
      }
      sbPad.append('-');
      sbPad.append('\n');

      pw.print(sbPad.toString());
      pw.print(sbNames.toString());
      pw.print(sbPad.toString());

      while (results.next())  {
        StringBuilder sb = new StringBuilder();
        sb.append(LEFT_BORDER);
        for (int i = 1; i < cols + 1; i++) {
          printPadded(sb, results.getString(i), colWidths[i - 1]);
          sb.append(COL_SEPARATOR);
        }
        sb.append('\n');
        pw.print(sb.toString());
      }

      pw.print(sbPad.toString());
    } catch (SQLException sqlException) {
      LoggingUtils.logAll(LOG, "Error reading from database: ", sqlException);
    }
  }

}

