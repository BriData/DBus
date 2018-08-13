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

package com.creditease.dbus.manager;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.creditease.dbus.common.utils.DBConfiguration;

/**
 * Manages direct connections to MySQL databases
 * so we can use mysqldump to get really fast dumps.
 */
// refactor connection 2017.11.29
@Deprecated
public class DirectMySQLManager
    extends MySQLManager {

  public static final Log LOG = LogFactory.getLog(
      DirectMySQLManager.class.getName());

  public DirectMySQLManager(final DBConfiguration options, String conString) {
    super(options,conString);
  }

  /**
   * Import the table into HDFS by using mysqldump to pull out the data from
   * the database and upload the files directly to HDFS.
   */
//  @Override
//  public void importTable(com.cloudera.sqoop.manager.ImportJobContext context)
//      throws IOException, ImportException {
//
//    context.setConnManager(this);
//    if (context.getOptions().getColumns() != null) {
//      LOG.warn("Direct-mode import from MySQL does not support column");
//      LOG.warn("selection. Falling back to JDBC-based import.");
//      // Don't warn them "This could go faster..."
//      MySQLManager.markWarningPrinted();
//      // Use JDBC-based importTable() method.
//      super.importTable(context);
//      return;
//    }
//
//    String tableName = context.getTableName();
//    String jarFile = context.getJarFile();
//    DBConfiguration options = context.getOptions();
//
//    MySQLDumpImportJob importer = null;
//    try {
//      importer = new MySQLDumpImportJob(options, context);
//    } catch (ClassNotFoundException cnfe) {
//      throw new IOException("Could not load required classes", cnfe);
//    }
//
//    String splitCol = getSplitColumn(options, tableName);
//    if (null == splitCol && options.getNumMappers() > 1) {
//      // Can't infer a primary key.
//      throw new ImportException("No primary key could be found for table "
//          + tableName + ". Please specify one with --split-by or perform "
//          + "a sequential import with '-m 1'.");
//    }
//
//    LOG.info("Beginning mysqldump fast path import");
//
//    if (options.getFileLayout() != DBConfiguration.FileLayout.TextFile) {
//      // TODO(aaron): Support SequenceFile-based load-in.
//      LOG.warn("File import layout " + options.getFileLayout()
//          + " is not supported by");
//      LOG.warn("MySQL direct import; import will proceed as text files.");
//    }
//
//    importer.runImport(tableName, jarFile, splitCol, options.getConf());
//  }

  /**
   * Export the table from HDFS by using mysqlimport to insert the data
   * back into the database.
   */
//  @Override
//  public void exportTable(com.cloudera.sqoop.manager.ExportJobContext context)
//      throws IOException, ExportException {
//    context.setConnManager(this);
//    MySQLExportJob exportJob = new MySQLExportJob(context);
//    exportJob.runExport();
//  }
//
//  public void upsertTable(com.cloudera.sqoop.manager.ExportJobContext context)
//      throws IOException, ExportException {
//    throw new ExportException("MySQL direct connector does not support upsert"
//      + " mode. Please use JDBC based connector (remove --direct parameter)");
//  }

  @Override
  public boolean supportsStagingForExport() {
    return false;
  }
}

