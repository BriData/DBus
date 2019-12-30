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


package com.creditease.dbus.handler;

import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.common.FullPullConstants;
import com.creditease.dbus.common.bean.DBConfiguration;
import com.creditease.dbus.common.bean.HdfsConnectInfo;
import com.creditease.dbus.common.bean.ProgressInfo;
import com.creditease.dbus.common.format.DataDBInputSplit;
import com.creditease.dbus.common.format.InputSplit;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.DbusMessage;
import com.creditease.dbus.commons.MetaWrapper;
import com.creditease.dbus.commons.ZkService;
import com.creditease.dbus.enums.DbusDatasourceType;
import com.creditease.dbus.helper.DBHelper;
import com.creditease.dbus.helper.FullPullHelper;
import com.creditease.dbus.manager.DBRecordReader;
import com.creditease.dbus.manager.GenericSqlManager;
import com.creditease.dbus.utils.TimeUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This is Description
 *
 * @author xiancangao
 * @date 2019/01/12
 */
public class DefaultPullHandler extends PullHandler {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private String sinkType;
    private long finishedRows = 0;
    private long sendRowMemSize = 0;
    private long sendRowsCnt = 0;
    private int umslength = 0;
    private AtomicLong sendCnt = new AtomicLong(0);
    private AtomicLong recvCnt = new AtomicLong(0);
    private AtomicBoolean isError = new AtomicBoolean(false);

    @Override
    public void doPullingProcess(Tuple input, String reqString, InputSplit inputSplit, Long splitIndex) throws Exception {
        fetchingData(input, reqString, (DataDBInputSplit) inputSplit, splitIndex);
    }

    private void fetchingData(Tuple input, String reqString, DataDBInputSplit inputSplit, Long splitIndex) throws Exception {
        Long startTime = System.currentTimeMillis();
        ResultSet rs = null;
        GenericSqlManager dbManager = null;
        ZkService localZkService = null;
        long emitRows = 0;
        try {
            JSONObject reqJson = JSONObject.parseObject(reqString);
            JSONObject payloadJson = reqJson.getJSONObject(FullPullConstants.REQ_PAYLOAD);
            int batchNo = payloadJson.getIntValue(FullPullConstants.REQ_PAYLOAD_BATCH_NO);

            DBConfiguration dbConf = FullPullHelper.getDbConfiguration(reqString);
            String dsType = dbConf.getString(DBConfiguration.DataSourceInfo.DS_TYPE);
            sinkType = dbConf.getString(DBConfiguration.SINK_TYPE);

            String logicalTableName = dbConf.getInputTableName();
            String dsKey = FullPullHelper.getDataSourceKey(reqJson);

            MetaWrapper metaInDbus = DBHelper.getMetaInDbus(reqString);
            if (metaInDbus == null) {
                logger.error("Get table meta from dbusMgr is null.");
                throw new RuntimeException("Get table meta from dbusMgr is null");
            }

            List<List<Object>> tuples = new ArrayList<>();
            long lastUpdatedMonitorTime = System.currentTimeMillis();
            long monitorTimeInterval = FullPullConstants.HEARTBEAT_MONITOR_TIME_INTERVAL_DEFAULT_VAL;
            Properties commonProps = FullPullHelper.getFullPullProperties(FullPullConstants.COMMON_CONFIG, true);
            String monitorTimeIntervalConf = commonProps.getProperty(FullPullConstants.HEARTBEAT_MONITOR_TIME_INTERVAL);
            if (monitorTimeIntervalConf != null) {
                monitorTimeInterval = Long.valueOf(monitorTimeIntervalConf);
            }

            String monitorNodePath = FullPullHelper.getMonitorNodePath(reqString);
            String opTs = dbConf.getString(DBConfiguration.DATA_IMPORT_OP_TS);

            localZkService = reloadZkService();
            String targetTableName = inputSplit.getTargetTableName();
            String resultKey = dbConf.getKafkaKey(reqString, targetTableName);
            String outputVersion = FullPullHelper.getOutputVersion();
            String resultTopic = (String) dbConf.get(DBConfiguration.DataSourceInfo.OUTPUT_TOPIC);

            dbManager = FullPullHelper.getDbManager(dbConf, dbConf.getString(DBConfiguration.DataSourceInfo.URL_PROPERTY_READ_ONLY));
            DBRecordReader dbRecordReader = DBHelper.getRecordReader(dbManager, dbConf, inputSplit, logicalTableName);
            rs = dbRecordReader.queryData(splitIndex.toString());
            ResultSetMetaData rsmd = rs.getMetaData();
            this.umslength = getEmptyUMSlength(outputVersion, reqString, dbConf, rsmd, metaInDbus, inputSplit.getTargetTableName(), batchNo);
            this.sendRowMemSize = umslength;
            int columnCount = rsmd.getColumnCount();
            while (rs.next()) {
                List<Object> rowDataValues = new ArrayList<>();
                String pos = payloadJson.getString(FullPullConstants.REQ_PAYLOAD_POS);
                rowDataValues.add(pos);
                rowDataValues.add(opTs);
                rowDataValues.add("i");
                //1.3 or later include _ums_uid_
                if (!outputVersion.equals(Constants.VERSION_12)) {
                    long uniqId = localZkService.nextValue(dbConf.buildNameSpaceForZkUidFetch(reqString));
                    rowDataValues.add(String.valueOf(uniqId)); // 全局唯一 _ums_uid_
                }
                emitRows++;
                sendRowsCnt++;
                finishedRows++;
                for (int i = 1; i <= columnCount; i++) {
                    sendRowMemSize = processResultSet(rs, dsType, sendRowMemSize, rsmd, rowDataValues, i);
                }
                tuples.add(rowDataValues);
                sendData(false, reqString, inputSplit, batchNo, dbConf, metaInDbus, tuples, resultKey, outputVersion, resultTopic, rsmd);

                if (isError.get()) {
                    throw new Exception("kafka send exception!");
                }

                long updatedMonitorInterval = (System.currentTimeMillis() - lastUpdatedMonitorTime) / 1000;
                if (updatedMonitorInterval > monitorTimeInterval) {
                    //1. 隔一段时间刷新monitor zk状态,最后一个参数：一片尚未完成，计数0
                    emitMonitorState(input, reqString, monitorNodePath, emitRows, 0);
                    logger.info("[pull bolt] {}, pull_index{}: current shard has finished {} rows. cost time {}", dsKey, splitIndex, finishedRows, TimeUtils.formatTime(System.currentTimeMillis() - startTime));
                    lastUpdatedMonitorTime = System.currentTimeMillis();
                    emitRows = 0;

                    //2. 如果已经出现错误，跳过后来的tuple数据
                    ProgressInfo progressInfo = FullPullHelper.getMonitorInfoFromZk(localZkService, monitorNodePath);
                    if (progressInfo.getErrorMsg() != null) {
                        logger.error("Get progressInfo failed，skipped index:" + splitIndex);
                        collector.fail(input);
                        return;
                    }
                }
            }

            if (tuples.size() > 0) {
                sendData(true, reqString, inputSplit, batchNo, dbConf, metaInDbus, tuples, resultKey, outputVersion, resultTopic, rsmd);
            }

            boolean isFinished = isSendFinished(sendCnt, recvCnt);
            if (isFinished) {
                //最后一个参数：完成一片，计数1
                emitMonitorState(input, reqString, monitorNodePath, emitRows, 1);
                logger.info("[pull bolt] {}, pull_index{}, current shard has finished {} rows. cost time {}", dsKey, splitIndex, finishedRows, TimeUtils.formatTime(System.currentTimeMillis() - startTime));
                collector.ack(input);
            } else {
                throw new Exception("Waiting kafka ack timeout!");
            }

            logger.info("[pull bolt] {}, pull_index{}, finished {} rows", dsKey, splitIndex, emitRows);
        } catch (Exception e) {
            logger.error("Exception happened when fetching data.", e);
            throw e;
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (dbManager != null) {
                    dbManager.close();
                }
                if (localZkService != null) {
                    localZkService.close();
                }
            } catch (Exception e) {
                logger.error("close dbManager error.", e);
            }
        }
    }

    private void sendData(boolean isLast, String reqString, DataDBInputSplit inputSplit, int batchNo, DBConfiguration dbConf,
                          MetaWrapper metaInDbus, List<List<Object>> tuples, String resultKey, String outputVersion,
                          String resultTopic, ResultSetMetaData rsmd) throws Exception {
        long startTime = System.currentTimeMillis();
        if (FullPullConstants.SINK_TYPE_KAFKA.equals(sinkType)) {
            if (isLast || isKafkaSend(sendRowMemSize, sendRowsCnt)) {
                sendRowMemSize = umslength;
                sendRowsCnt = 0;
                DbusMessage dbusMessage = buildResultMessage(outputVersion, tuples, reqString, dbConf, rsmd, metaInDbus,
                        inputSplit.getTargetTableName(), batchNo);
                sendMessageToKafka(resultTopic, resultKey, dbusMessage, sendCnt, recvCnt, isError);
                tuples.clear();
            }
        } else {
            if (isLast || sendRowMemSize >= hdfsSendBatchSize) {
                HdfsConnectInfo hdfsConnectInfo = null;
                try {
                    sendRowMemSize = umslength;
                    DbusMessage dbusMessage = buildResultMessage(outputVersion, tuples, reqString, dbConf, rsmd, metaInDbus,
                            inputSplit.getTargetTableName(), batchNo);
                    byte[] bytes = (dbusMessage.toString() + "\n").getBytes("utf-8");
                    hdfsConnectInfo = getOutPutStream(dbConf, reqString, bytes.length);
                    FSDataOutputStream outputStream = hdfsConnectInfo.getFsDataOutputStream();
                    outputStream.write(bytes);
                    outputStream.hsync();
                    tuples.clear();
                    logger.info("[pull bolt] send to hdfs data size:{}, cost time {}", bytes.length, TimeUtils.formatTime(System.currentTimeMillis() - startTime));
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                    String filePath = hdfsConnectInfo.getFilePath();
                    try {
                        FullPullHelper.setHdfsConnectInfo(reqString, null);
                        hdfsConnectInfo.clear();
                        logger.info("[pull bolt] close error FSDataOutputStream ,filePath {}.", filePath);
                    } catch (IOException e1) {
                        logger.error("close FSDataOutputStream error,{}", filePath, e1);
                    }
                }
            }
        }
    }

    private HdfsConnectInfo getOutPutStream(DBConfiguration dbConf, String reqString, int len) throws Exception {
        HdfsConnectInfo hdfsConnectInfo = FullPullHelper.getHdfsConnectInfo(reqString);
        FSDataOutputStream fsDataOutputStream = null;
        if (hdfsConnectInfo == null) {
            hdfsConnectInfo = new HdfsConnectInfo();
        } else {
            FSDataOutputStream outputStream = hdfsConnectInfo.getFsDataOutputStream();
            if (isCreateNewFile(outputStream, len)) {
                if (outputStream != null) {
                    outputStream.close();
                    logger.info("[pull bolt] close FSDataOutputStream ,filePath {}.", hdfsConnectInfo.getFilePath());
                }
            } else {
                fsDataOutputStream = outputStream;
            }
        }
        if (fsDataOutputStream == null) {
            try {
                String filePath = dbConf.getString(DBConfiguration.HDFS_TABLE_PATH) + FullPullHelper.getHdfsFileName(reqString);
                filePath = filePath.toLowerCase();
                logger.info("[pull bolt] will create FSDataOutputStream  with filePath {}", filePath);
                fsDataOutputStream = fileSystem.create(new Path(filePath));
                hdfsConnectInfo.setFilePath(filePath);
                hdfsConnectInfo.setFsDataOutputStream(fsDataOutputStream);
                logger.info("[pull bolt] create FSDataOutputStream success ,filePath {}", filePath);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
        FullPullHelper.setHdfsConnectInfo(reqString, hdfsConnectInfo);
        return hdfsConnectInfo;
    }

    private boolean isCreateNewFile(FSDataOutputStream fsDataOutputStream, int len) {
        if (fsDataOutputStream == null) {
            return true;
        } else {
            try {
                return (fsDataOutputStream.getPos() + len) > hdfsFileMaxSize;
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
            return true;
        }
    }

    private long processResultSet(ResultSet rs, String dsType, long dealRowMemSize, ResultSetMetaData rsmd,
                                  List<Object> rowDataValues, int i) throws SQLException, UnsupportedEncodingException {
        String columnTypeName = rsmd.getColumnTypeName(i);
        // 关于时间的值需要特别处理一下。否则，可能会导致DbusMessageBuilder private void validateAndConvert(Object[] tuple)方法抛异常
        // 例如 Year类型，库里值为2016，不做特别处理的话， 从rs读出来的值会被自动转成2016-01-01。按dbus映射规则，DbusMessageBuilder 将year按int处理时，会出错
        // TODO: timezone
        switch (columnTypeName) {
            case "DATE":
                if (rs.getObject(i) != null) {
                    if (dsType.equalsIgnoreCase(DbusDatasourceType.MYSQL.name()) ||
                            dsType.equalsIgnoreCase(DbusDatasourceType.ORACLE.name())) {
                        rowDataValues.add(rs.getDate(i) + " " + rs.getTime(i));
                    } else {
                        //DB2 rs.getTime(i)会报错(数据转换无效：所请求转换的结果列类型错误。 ERRORCODE=-4461, SQLSTATE=42815)
                        rowDataValues.add(rs.getDate(i));
                    }
                } else {
                    rowDataValues.add(rs.getObject(i));
                }
                break;
            case "YEAR":
                if (rs.getObject(i) != null) {
                    Date date = (Date) (rs.getObject(i));
                    Calendar cal = Calendar.getInstance();
                    cal.setTime(date);
                    rowDataValues.add(cal.get(Calendar.YEAR));
                } else {
                    rowDataValues.add(rs.getObject(i));
                }
                break;
            case "TIME":
                if (rs.getTime(i) != null) {
                    rowDataValues.add(rs.getTime(i).toString());
                } else {
                    rowDataValues.add(rs.getObject(i));
                }
                break;
            case "DATETIME":
            case "TIMESTAMP":
                if (rs.getTimestamp(i) != null) {
                    //rowDataValues.add(rs.getTimestamp(i).toString());
                    String timeStamp = "";
                    if (dsType.toUpperCase().equals(DbusDatasourceType.MYSQL.name())
                            || dsType.equalsIgnoreCase(DbusDatasourceType.DB2.name())
                    ) {
                        timeStamp = toMysqlTimestampString(rs.getTimestamp(i), rsmd.getPrecision(i));
                    } else if (dsType.toUpperCase().equals(DbusDatasourceType.ORACLE.name())) {
                        timeStamp = toOracleTimestampString(rs.getTimestamp(i), rsmd.getScale(i));
                    } else {
                        throw new RuntimeException("Wrong Database type.");
                    }
                    rowDataValues.add(timeStamp);
                } else {
                    Object val = rs.getObject(i);
                    if (dsType.toUpperCase().equals(DbusDatasourceType.MYSQL.name()) && rsmd.isNullable(i) != 1 && val == null) {
                        // JAVA连接MySQL数据库，在操作值为0的timestamp类型时不能正确的处理，而是默认抛出一个异常，就是所见的：java.sql.SQLException: Cannot convert value '0000-00-00 00:00:00' from column 7 to TIMESTAMP。
                        // DBUS处理策略：在JDBC连接串配置属性：zeroDateTimeBehavior=convertToNull，来避免异常。
                        // 但当对应列约束为非空时，转换成null，后续逻辑校验通不过。所以对于mysql非空timestamp列，当得到值为null时，一定是发生了从 '0000-00-00 00:00:00'到null的转换。为了符合后续逻辑校验，此处强制将null置为'0000-00-00 00:00:00'。
                        val = "0000-00-00 00:00:00";
                    }
                    rowDataValues.add(val);
                }
                break;
            case "BINARY":
            case "VARBINARY":
            case "TINYBLOB":
            case "BLOB":
                if (rs.getObject(i) != null) {
                    // 对于上述四种类型，根据canal文档https://github.com/alibaba/canal/issues/18描述，针对blob、binary类型的数据，使用"ISO-8859-1"编码转换为string
                    // 为了和增量保持一致，对于这四种类型，全量也需做特殊处理：读取bytes并用ISO-8859-1编码转换成string。
                    // 后续DbusMessageBuilder  void validateAndConvert(Object[] tuple)方法会统一按ISO-8859-1编码处理全量/增量数据。
                    // 另，测试发现，这样的转换已“最大程度”和增量保持了一致。但对于BINARY类型，仍有一点差异。具体如下：
                    // 设数据库有一列名为filed_binay，类型为binary(200)，插入数据为： "test_binary中文测试转换,，dbus将此类型转换为base64编码 "。
                    // 在不加密的情况下，同样的数据，落到EDP mysql后，增量全量的数据能对上，如下：
                    // 增量：test_binary中文测试转换,，dbus将此类型转换为base64编码
                    // 全量：test_binary中文测试转换,，dbus将此类型转换为base64编码
                    // 在hash_md5加密的情况下，增量全量的数据对不上。数据如下：
                    // 增量：g6w
                    // 全量：??svom_u
                    // 原因：filed_binay 列类型为binary(200)，插入字符串长度没达到200，数据库将内容自动补齐至200。
                    // 增量通过canal读取原始数据时，读到的数据忽略了补齐部分。
                    // 全量通过JDBC读取原始数据时，读到的是包含补齐部分的数据，长度200。
                    // 对于这种补齐的情况，不加密处理的话，肉眼观察，内容编码/解码没区别。
                    // 用hd5加密的话，hd5加密结果会不同。
                    // 对于这个情况，暂时忽略搁置。
                    rowDataValues.add(new String(rs.getBytes(i), "ISO-8859-1"));
                } else {
                    rowDataValues.add(rs.getObject(i));
                }
                break;
            //暂时只支持BIT(0)~BIT(8)，对于其它的(n>8) BIT(n)，需要增加具体的处理
            case "BIT":
                byte[] value = rs.getBytes(i);
                if (value != null && value.length > 0)
                    rowDataValues.add(value[0] & 0xFF);
                else
                    rowDataValues.add(rs.getObject(i));
                break;
            default:
                rowDataValues.add(rs.getObject(i));
                break;
        }
        dealRowMemSize += String.valueOf(rs.getObject(i)).getBytes().length;
        return dealRowMemSize;
    }

    /**
     * The default java.sql.Timestamp.toString() does not equal to mysql timestamp.
     * Below code lines are copied from java.sql.Timestamp.toString(), just changed the nanos part.
     */
    public String toMysqlTimestampString(java.sql.Timestamp ts, int precision) {
        // Mysql's metaData.getScale() always return 0. We use getPrecision() to estimate scale.
        // By testing, the precision of datetime is 19, which means the length "2010-01-02 10:12:23" is 19.
        // the precision of datetime(1) is 21, which means the length "2010-01-02 10:12:23.1" is 21.
        // the precision of datetime(2) is 22, which means the length "2010-01-02 10:12:23.12" is 22. and so on.
        // So, we use 20 as key to distinguish datetime from datetime(x).
        int meta = precision - 20;
        if (meta > 6) {
            throw new RuntimeException("unknow useconds meta : " + meta);
        }

        int year = ts.getYear() + 1900;
        int month = ts.getMonth() + 1;
        int day = ts.getDate();
        int hour = ts.getHours();
        int minute = ts.getMinutes();
        int second = ts.getSeconds();
        int nanos = ts.getNanos();
        String yearString;
        String monthString;
        String dayString;
        String hourString;
        String minuteString;
        String secondString;
        String nanosString;
        String zeros = "000000000";
        String yearZeros = "0000";
        StringBuffer timestampBuf;

        if (year < 1000) {
            // Add leading zeros
            yearString = "" + year;
            yearString = yearZeros.substring(0, (4 - yearString.length())) +
                    yearString;
        } else {
            yearString = "" + year;
        }
        if (month < 10) {
            monthString = "0" + month;
        } else {
            monthString = Integer.toString(month);
        }
        if (day < 10) {
            dayString = "0" + day;
        } else {
            dayString = Integer.toString(day);
        }
        if (hour < 10) {
            hourString = "0" + hour;
        } else {
            hourString = Integer.toString(hour);
        }
        if (minute < 10) {
            minuteString = "0" + minute;
        } else {
            minuteString = Integer.toString(minute);
        }
        if (second < 10) {
            secondString = "0" + second;
        } else {
            secondString = Integer.toString(second);
        }

        //make nanoString length as 9.
        if (nanos == 0) {
            nanosString = zeros.substring(0, 9);
        } else {
            nanosString = Integer.toString(nanos);
        }

        // Add leading zeros
        nanosString = zeros.substring(0, (9 - nanosString.length())) +
                nanosString;

        if (meta <= 0) {
            nanosString = "";
        } else {
            //truncate nanoString by meta
            nanosString = "." + nanosString.substring(0, meta);
        }

        // do a string buffer here instead.
        timestampBuf = new StringBuffer(20 + nanosString.length());
        timestampBuf.append(yearString);
        timestampBuf.append("-");
        timestampBuf.append(monthString);
        timestampBuf.append("-");
        timestampBuf.append(dayString);
        timestampBuf.append(" ");
        timestampBuf.append(hourString);
        timestampBuf.append(":");
        timestampBuf.append(minuteString);
        timestampBuf.append(":");
        timestampBuf.append(secondString);
        timestampBuf.append(nanosString);

        return (timestampBuf.toString());
    }

    public String toOracleTimestampString(java.sql.Timestamp ts, int scale) {
        String timeStamp = ts.toString();
        String timeStampLastPart = timeStamp.substring(timeStamp.lastIndexOf(".") + 1, timeStamp.length());
        int needAdd = scale - timeStampLastPart.length();
        while (needAdd > 0) {
            timeStamp += "0";
            needAdd--;
        }
        return timeStamp;
    }

}
