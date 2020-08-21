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


package com.creditease.dbus.stream.mysql.appender.protobuf.protocol;

import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.Header;
import com.creditease.dbus.commons.PropertiesHolder;
import com.creditease.dbus.commons.ZkContentHolder;
import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.appender.utils.ControlMessageUtils;
import com.creditease.dbus.stream.common.appender.utils.Utils;
import com.creditease.dbus.stream.mysql.appender.exception.ProtobufParseException;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

public class EntryHeader implements Serializable {
    private Logger logger = LoggerFactory.getLogger(getClass());
    private Header header;

    private String localTable;
    private String partitionTable;
    private String pos;
    private String tsTime;

    // 为了解决由于mysql备库log文件编号变小导致生成的ums_id变小，
    // 导致wormhole做幂等无法写入到sink端的问题
    private long logFileNumCompensation = 0;

    public EntryHeader(Header header) {
        this.header = header;
        localTable = StringUtils.substringBefore(this.header.getTableName(), ".");
        String[] name = StringUtils.split(this.header.getTableName(), ".");
        if (name.length == 1) {
            localTable = name[0];
            partitionTable = Constants.PARTITION_TABLE_DEFAULT_NAME;
        } else if (name.length == 2) {
            localTable = name[0];
            partitionTable = name[1];
        } else {
            throw new ProtobufParseException("protobuf parser table error!");
        }

        //计算pos
        String logFileSuffix = StringUtils.substringAfterLast(header.getLogfileName(), ".").replaceAll("\\D", "");
        //header.getLogfileOffset()  出现过负值
        //Long.MAX_VALUE为9223372036854775807，长度为19位，去除拼接后的logfilesuffix6位长度，留给logfileoffset的长度为13位
        //因此当header.getLogfileOffset()出现负值时，使用999999999999-header.getLogfileOffset()得到一个正数，这样也可避免
        //与其它offset值出现重叠，而导致被误认为此消息已处理过。前提是一个mysql_bin文件的offset值不会有这么大，验证过一个mysql_bin文件，
        //最大的offset为5595591889。
        String logfileOffset;
        if (header.getLogfileOffset() < 0) {
            logfileOffset = String.format("%013d", -header.getLogfileOffset());
            logger.warn("EntryHeader: logfileOffset has a negative value {}, logfile No. {}", header.getLogfileOffset(), logFileSuffix);
        } else {
            logfileOffset = String.format("%013d", header.getLogfileOffset());
        }
        this.logFileNumCompensation = getLogFileNumCompensation();
        this.pos = StringUtils.join(new Object[]{Long.parseLong(logFileSuffix) + this.logFileNumCompensation, logfileOffset});

        //	System.out.println("logFileSuffix"+logFileSuffix+", logfileOffset"+logfileOffset+", pos"+this.pos);

        //计算时间--将excuteTime从毫秒数转换为指定格式的字符串
        Timestamp ts = new Timestamp(header.getExecuteTime());
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        Date date = ts;
        tsTime = format.format(date);
    }

    private long getLogFileNumCompensation() {
        try {
            Properties properties = PropertiesHolder.getProperties(Constants.Properties.CONFIGURE);
            return Long.parseLong(properties.getOrDefault(Constants.ConfigureKey.LOGFILE_NUM_COMPENSATION, 0).toString());
        } catch (Exception e) {
            throw new RuntimeException("获取配置文件失败", e);
        }
    }

    public Header getHeader() {
        return header;
    }

    public void setHeader(Header header) {
        this.header = header;
    }

    public EventType getOperType() {
        return header.getEventType();
    }

    public boolean isInsert() {
        return header.getEventType() == EventType.INSERT;
    }

    public boolean isUpdate() {
        return header.getEventType() == EventType.UPDATE;
    }

    public boolean isDelete() {
        return header.getEventType() == EventType.DELETE;
    }

    public boolean isTruncate() {
        return header.getEventType() == EventType.TRUNCATE;
    }

    public boolean isAlter() {
        return header.getEventType() == EventType.ALTER;
    }

    public void setPos(String pos) {
        this.pos = pos;
    }

    public void setLastPos(long lastPos) {
        if (header.getLogfileOffset() < 0) {
            this.pos = String.valueOf(lastPos);
        }
    }

    public String getPos() {
        return this.pos;
    }

    public String getSchemaName() {
        return header.getSchemaName();
    }

    /**
     * 获取逻辑表名，header中的表名格式为：逻辑表名.分区表名
     *
     * @return
     */
    public String getTableName() {
        return localTable;
    }

    /**
     * 获取分区表名
     *
     * @return
     */
    public String getPartitionTableName() {
        return partitionTable;
    }

    public long getExecuteTime() {
        return header.getExecuteTime();
    }

    public String getTsTime() {
        return tsTime;
    }

    public static void main(String[] args) {
        String str = "abcd.aaa";
        String[] name = StringUtils.split(str, ".");
        System.out.println(Arrays.toString(name));

        //过滤字符串中所有非数字字符
        String testStr = "1281812ewew-121-";
        System.out.println(testStr.replaceAll("\\D", ""));
        System.out.println(StringUtils.join(new String[]{"000006", "5212"}));


        String logFileSuffix = StringUtils.substringAfterLast("mysql_bin.000006-", ".").replaceAll("\\D", "");
        String logfileOffset = String.valueOf(-5212);
        String pos = StringUtils.join(new String[]{logFileSuffix, logfileOffset});
        System.out.println(pos);
        System.out.println(Long.MAX_VALUE);
        long offset = -5212;
        System.out.println(100000000000L - offset);

        long testValue = 1234;
        System.out.println(String.format("%013d", testValue));
    }
}
