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
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.creditease.dbus.common.utils;

import com.creditease.dbus.common.DataPullConstants;
import com.creditease.dbus.common.splitters.DateSplitter;
import com.creditease.dbus.common.utils.DataDrivenDBInputFormat.DataDrivenDBInputSplit;
import com.creditease.dbus.manager.GenericJdbcManager;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.Arrays;
import java.util.HashMap;

/**
 * A RecordReader that reads records from a SQL table.
 * Emits LongWritables containing the record number as
 * key and DBWritables as value.
 */
public class DBRecordReader<T extends DBWritable> {

    private Logger LOG = LoggerFactory.getLogger(getClass());

//  private ResultSet results = null;

//  private Class<T> inputClass;

    private DataDrivenDBInputFormat.DataDrivenDBInputSplit split;

    private long pos = 0;

//  private LongWritable key = null;

    private T value = null;

    private GenericJdbcManager manager;

    //private Connection connection;

    //protected PreparedStatement statement;

    private DBConfiguration dbConf;

    private String[] fieldNames;

    private String tableName;

    /**
     * @throws SQLException
     */
    // CHECKSTYLE:OFF
    // TODO (aaron): Refactor constructor to take fewer arguments
    public DBRecordReader(GenericJdbcManager manager,
                          DBConfiguration dbConfig, DataDrivenDBInputSplit split, String[] fields,
                          String table)
            throws SQLException {
        this.manager = manager;
        this.dbConf = dbConfig;
        this.split = split;
        if (fields != null) {
            this.fieldNames = Arrays.copyOf(fields, fields.length);
        }
        this.tableName = table;
    }
    // CHECKSTYLE:ON

//	public static String convert2Nchar(String originString,int length) {
//	  StringBuffer stringBuffer=  new StringBuffer(originString);
//	  int size= originString.length();
//	  if ( size< length) {
//		  for( int i=1; i<= length- size; i++){
//		  stringBuffer.append( " ");
//		  }
//		  originString= stringBuffer.toString();
//	  }
//	  return originString;
//	}

    public HashMap<String, HashMap<String, Object>> queryMetaData() throws Exception {
        Connection conn = null;
        PreparedStatement stat = null;
        ResultSet res = null;
        try {
            String query = getMetaSql();
            conn = manager.getConnection();
            stat = conn.prepareStatement(query);
            res = stat.executeQuery();
            HashMap<String, HashMap<String, Object>> result = new HashMap<>();
            ResultSetMetaData metaData = res.getMetaData();
            int columnCount = metaData.getColumnCount();
            while (res.next()) {
                HashMap<String, Object> meta = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    meta.put(metaData.getColumnName(i), res.getObject(i));
                }
                result.put(res.getString("columnName"), meta);
            }
            return result;
        } catch (Exception e) {
            LOG.error("query oracle tabel meta error !", e);
            return null;
        } finally {
            manager.close();
            if (stat != null) {
                stat.close();
            }
            if (res != null) {
                res.close();
            }
        }
    }

    public String getMetaSql() {
        return null;
    }

    public ResultSet queryData(String datasourceType, String splitIndex) throws Exception {

        ResultSet rset = null;
        try {
            Object lowBound = this.split.getLowerValue();
            Object upperBound = this.split.getUpperValue();

            if (lowBound instanceof String && upperBound instanceof String) {
                String lower = (String) lowBound;
                String upper = (String) upperBound;
                for (int i = 0; i < lower.length(); i++) {
                    if (lower.charAt(i) > (int) 0xffff) {
                        throw new Exception("Exception:lower char is wrong" + lower.charAt(i));
                    }
                }
                for (int i = 0; i < upper.length(); i++) {
                    if (upper.charAt(i) > (int) 0xffff) {
                        throw new Exception("Exception:upper char is wrong" + upper.charAt(i));
                    }
                }
            }

            String query = getSelectQuery();

            PreparedStatement statement = manager.prepareStatement(query);
            // cond 不为 is null的时候，才用set 条件值。另：lower is null, upper一定也is null.所以不用两个都判断。
            if (!DataPullConstants.QUERY_COND_IS_NULL.equals(this.split.getLowerOperator())) {
                if (this.split.getSqlType() == Types.DATE || this.split.getSqlType() == Types.TIME || this.split.getSqlType() == Types.TIMESTAMP) {
                    // Mysql：Time类型，TIMESTAMP类型，Date类型：都可以用这种方式处理.
                    // Oracle:沒有Time類型。TIMESTAMP类型可用这种方式处理，Date类型，不能用这种方式处理（会丢失其所含的时分秒毫秒信息），下面会单独处理。
                    lowBound = DateSplitter.longToDate((long) this.split.getLowerValue(), this.split.getSqlType());
                    upperBound = DateSplitter.longToDate((long) this.split.getUpperValue(), this.split.getSqlType());
                }
                /*这段代码已经不会走到了。对于Oracle Types.DATE，DateSplitter类中已经将其强制转换成Types.TIMESTAMP。暂时保留便于了解相关细节。
                // Date类型，oracle需特殊处理下。Oracle的Date类型不仅保存年月日，还能保存时分秒甚至毫秒信息。
                // 但Oracle通过resultSet.getObject获取时间时，可能遭到截断，丢失时分秒（http://www.myexception.cn/database/1044846.html）
                // 例：对于 2008-06-13 13:48:21.0， 9i/11g返回2008-06-13 13:48:21.0；10g返回2008-06-13
                // 这里提到的版本指数据库服务器版本。同样版本的jdbc，连不同环境的Oracle服务器，同样的类型和数据，返回值不一样。
                // 提示说可用prop.setProperty("oracle.jdbc.V8Compatible" ,"true");解决。在有问题的环境验证，没解决问题。所以采取了强制转换为Types.TIMESTAMP的方式处理。
                if (datasourceType.toUpperCase().equals(DbusDatasourceType.ORACLE.name()) && this.split.getSqlType() == Types.DATE) {
                    LOG.info("DbusDatasourceType.ORACLE------------ SqlType{}  ",this.split.getSqlType());
                    SimpleDateFormat dfs = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                    lowBound = dfs.format(new java.util.Date((long)this.split.getLowerValue()));
                    upperBound = dfs.format(new java.util.Date((long)this.split.getUpperValue()));
                    LOG.info("lower: {} and upper: {}.",lowBound,upperBound);
                }*/
                statement.setObject(1, lowBound, this.split.getSqlType());
                statement.setObject(2, upperBound, this.split.getSqlType());
            }

            LOG.info("pull_index{}: Query Begin: {}, with cond lower: {} and upper: {}.", splitIndex, query, lowBound, upperBound);

            int fetchSize = dbConf.getPrepareStatementFetchSize();
            statement.setFetchSize(fetchSize);
            LOG.info("pull_index{}: Using fetchSize for next query: {}", splitIndex, fetchSize);

            statement.setQueryTimeout(3600);
            LOG.info("pull_index{}: Using queryTimeout 3600 seconds", splitIndex);

            rset = statement.executeQuery();
            LOG.info("pull_index{}: executeQuery end! ", splitIndex);
            return rset;
        } catch (SQLException e) {
            LOG.error(e.getMessage(), e);
            LoggingUtils.logAll(LOG, "pull_index" + splitIndex + " Failed to list columns", e);
            return null;
        }
//        finally {
//            if (rset != null) {
//                try {
//                    rset.close();
//                }
//                catch (SQLException ex) {
//                    LoggingUtils.logAll(LOG, "Failed to close resultset", ex);
//                }
//            }
//            if (this.statement != null) {
//                try {
//                    this.statement.close();
//                }
//                catch (SQLException ex) {
//                    LoggingUtils.logAll(LOG, "Failed to close statement", ex);
//                }
//            }
//            // 关闭connecttion的事由druid去做
//             try {
//             close();
//             } catch (Exception e) {
//             // TODO Auto-generated catch block
//             LOG.error(e.getMessage(),e);
//             }
//        }
    }

    /**
     * Returns the query for selecting the records,
     * subclasses can override this for custom behaviour.
     *
     * @throws Exception
     */
    protected String getSelectQuery() throws Exception {
        StringBuilder query = new StringBuilder();
        query.append("SELECT ");
        if (null != fieldNames && fieldNames.length != 0) {
            for (int i = 0; i < fieldNames.length; i++) {
                query.append(fieldNames[i]);
                if (i != fieldNames.length - 1) {
                    query.append(", ");
                }
            }
        } else {
            throw new Exception("None supported columns found on current pulling target table");
        }
        query.append(" FROM ").append(getSplit().getTargetTableName());
        //      query.append(" AS ").append(getSplit().getTargetTableName()); //in hsqldb this is necessary
        if (StringUtils.isNotBlank(getSplit().getTablePartitionInfo())) {
            query.append(" PARTITION (").append(getSplit().getTablePartitionInfo()).append(") ");
        }
        if (split != null) {
            String condWithPlaceholder = split.getCondWithPlaceholder();
            query.append(" WHERE (").append(condWithPlaceholder).append(")");
        }
        String inputConditions = dbConf.getInputConditions();
        if(StringUtils.isNotBlank(inputConditions)){
            query.append(" AND (").append(inputConditions).append(")");
        }
        String orderBy = dbConf.getInputOrderBy();
        if (orderBy != null && orderBy.length() > 0) {
            query.append(" ORDER BY ").append(orderBy);
        }

//      query.append(" LIMIT 50");
        if (split.getLength() > 0 && split.getStart() > 0) {
            try {
                query.append(" LIMIT ").append(split.getLength());
                query.append(" OFFSET ").append(split.getStart());
            } catch (IOException ex) {
                // Ignore, will not throw.
            }
        }
        return query.toString();
    }

//  @Override
//    @Deprecated
//  public void close() throws IOException {
//    try {
//      if (null != results) {
//        results.close();
//      }
    // Statement.isClosed() is only available from JDBC 4
    // Some older drivers (like mysql 5.0.x and earlier fail with
    // the check for statement.isClosed()
//      if (null != statement) {
//        statement.close();
//      }
//   // 关闭connecttion的事由druid去做
//      if (null != connection && !connection.isClosed()) {
//        connection.commit();
//        connection.close();
//      }
//    } catch (SQLException e) {
//      throw new IOException(e);
//    }
//}

    public void initialize(InputSplit inputSplit)
            throws IOException, InterruptedException {
        //do nothing
    }

//  @Override
//  public LongWritable getCurrentKey() {
//    return key;
//  }

    //  @Override
    public T getCurrentValue() {
        return value;
    }

    /**
     * @deprecated
     */
//  @Deprecated
//  public T createValue() {
//    return ReflectionUtils.newInstance(inputClass, conf);
//  }

    /**
     * @deprecated
     */
    @Deprecated
    public long getPos() throws IOException {
        return pos;
    }

    /**
     * @deprecated Use {@link #nextKeyValue()}
     */
//  @Deprecated
//  public boolean next(LongWritable k, T v) throws IOException {
//    this.key = k;
//    this.value = v;
//    return nextKeyValue();
//  }

//  @Override
//  @Deprecated
//  public float getProgress() throws IOException {
//    return pos / (float)split.getLength();
//  }

//  @Override
//  public boolean nextKeyValue() throws IOException,Exception {
//    try {
////      if (key == null) {
////        key = new LongWritable();
////      }
////      if (value == null) {
////        value = createValue();
////      }
//      if (null == this.results) {
//        // First time into this method, run the query.
//        LOG.info("Working on split: " + split);
//        this.results = queryData(getSelectQuery());
//      }
//      if (!results.next()) {
//          results.close();
//        return false;
//      }
//
//      // Set the key field value as the output key value
////      key.set(pos + split.getStart());
//
//      value.readFields(results);
//
//      pos++;
//    } catch (SQLException e) {
//      LoggingUtils.logAll(LOG, e);
//      if (this.statement != null) {
//        try {
//          statement.close();
//        } catch (SQLException ex) {
//          LoggingUtils.logAll(LOG, "Failed to close statement", ex);
//        } finally {
//          this.statement = null;
//        }
//      }
//      if (this.connection != null) {
//        try {
//          connection.close();
//        } catch (SQLException ex) {
//          LoggingUtils.logAll(LOG, "Failed to close connection", ex);
//        } finally {
//          this.connection = null;
//        }
//      }
//      if (this.results != null) {
//        try {
//          results.close();
//        } catch (SQLException ex) {
//          LoggingUtils.logAll(LOG, "Failed to close ResultsSet", ex);
//        } finally {
//          this.results = null;
//        }
//      }
//
//      throw new IOException("SQLException in nextKeyValue", e);
//    }
//    catch (Exception e) {
//        throw new Exception("Exception happened. Maybe caused by getSelectQuery() func.", e);
//    }
//    return true;
//  }

    /**
     * @return true if nextKeyValue() would return false.
     */
//  @Deprecated
//  protected boolean isDone() {
//    try {
//      return this.results != null && results.isAfterLast();
//    } catch (SQLException sqlE) {
//      return true;
//    }
//  }
    protected DataDrivenDBInputFormat.DataDrivenDBInputSplit getSplit() {
        return split;
    }

    protected String[] getFieldNames() {
        return fieldNames;
    }

    protected String getTableName() {
        return tableName;
    }

    protected DBConfiguration getDBConf() {
        return dbConf;
    }
//    @Deprecated
//  protected Connection getConnection() {
//    return connection;
//  }
//    @Deprecated
//  protected void setConnection(Connection conn) {
//    connection = conn;
//  }

//  @Deprecated
//  protected PreparedStatement getStatement() {
//    return statement;
//  }
//
//  @Deprecated
//  protected void setStatement(PreparedStatement stmt) {
//    this.statement = stmt;
//  }

    public void setSplit(DataDrivenDBInputFormat.DataDrivenDBInputSplit split) {
        this.split = split;
    }

    /**
     * @return the configuration. Allows subclasses to access the configuration
     */
//  protected Configuration getConf(){
//    return conf;
//  }
}
