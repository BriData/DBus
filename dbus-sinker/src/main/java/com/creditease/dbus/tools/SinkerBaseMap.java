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


package com.creditease.dbus.tools;

import com.creditease.dbus.bean.HdfsOutputStreamInfo;
import com.creditease.dbus.bean.SinkerTopologyTable;
import com.creditease.dbus.cache.LocalCache;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.helper.DBHelper;
import com.creditease.dbus.helper.ZKHepler;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.*;
import java.util.stream.Collectors;

public class SinkerBaseMap {
    private Logger logger = LoggerFactory.getLogger(SinkerBaseMap.class);

    /**
     * 链接zk字符串
     */
    public String zkStr = null;

    /**
     * topology id
     */
    public String sinkerName = null;

    /**
     * sink type 目前仅支持hdfs-ums
     */
    public String sinkType = null;

    /**
     * storm topology 配置信息
     */
    public Map conf = null;

    /**
     * sinker 配置信息
     */
    public Properties sinkerConfProps;

    /**
     * hdfs 配置信息
     */
    public Properties hdfsConfProps;

    public int hsyncIntervals;

    public List<String> sourceTopics;

    public List<String> tableNamespaces;

    /**
     * 操作zk句柄
     */
    public ZKHepler zkHelper = null;

    public FileSystem fileSystem = null;

    public SinkerBaseMap(Map conf) {
        this.conf = conf;
        this.zkStr = (String) conf.get(Constants.ZOOKEEPER_SERVERS);
        this.sinkerName = (String) conf.get(Constants.TOPOLOGY_ID);
        this.sinkType = (String) conf.get(Constants.SINK_TYPE);
        logger.info("zk servers:{}, topology id:{}, sink type:{}", zkStr, sinkerName, sinkType);
    }

    public void init() throws Exception {
        this.zkHelper = new ZKHepler(zkStr, sinkerName);
        this.sinkerConfProps = zkHelper.loadSinkerConf(SinkerConstants.CONFIG);
        initSinkerTables();
    }

    private void initSinkerTables() throws Exception {
        Properties properties = zkHelper.getProperties(Constants.MYSQL_PROPERTIES_ROOT);
        List<SinkerTopologyTable> sinkerTopologyTables = DBHelper.querySinkerTables(properties, sinkerName);
        ArrayList<String> list = new ArrayList<>(sinkerTopologyTables.stream().map(table -> table.getTargetTopic()).collect(Collectors.toSet()));
        Collections.sort(list);
        this.sourceTopics = list;
        this.tableNamespaces = sinkerTopologyTables.stream().map(table -> {
            String dsName = StringUtils.isNotBlank(table.getAlias()) ? table.getAlias() : table.getDsName();
            return String.format("%s.%s.%s", dsName, table.getSchemaName(), table.getTableName());
        }).collect(Collectors.toList());
    }

    public void initSinker() throws Exception {
        if ("hdfs".equals(sinkType)) {
            closeHdfs();
            initHdfs();
        }
    }

    //清空不在该sinker处理的表的outputStream缓存
    public void closeHdfs() {
        try {
            Set<String> allKeys = LocalCache.getAllKeys();
            if (!allKeys.isEmpty()) {
                for (String key : allKeys) {
                    HdfsOutputStreamInfo hdfsOutputStreamInfo = LocalCache.get(key);
                    logger.info("[reload cmd] will close outStream use filePath {}", hdfsOutputStreamInfo.getFilePath());
                    hdfsOutputStreamInfo.clear();
                    LocalCache.remove(key);
                }
            }
            if (fileSystem != null) {
                fileSystem.close();
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    public void initHdfs() throws Exception {
        FileSystem fileSystem;
        Configuration conf = new Configuration();
        this.hdfsConfProps = this.zkHelper.loadSinkerConf(SinkerConstants.HDFS_CONFIG);
        this.hsyncIntervals = Integer.parseInt(hdfsConfProps.getProperty(SinkerConstants.HDFS_HSYNC_INTERVALS));
        String hdfsUrl = hdfsConfProps.getProperty(SinkerConstants.HDFS_URL);
        String hdoopUserName = hdfsConfProps.getProperty(SinkerConstants.HADOOP_USER_NAME);
        System.setProperty("HADOOP_USER_NAME", hdoopUserName);
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.setBoolean("fs.hdfs.impl.disable.cache", true);
        if (StringUtils.isNotBlank(hdfsUrl)) {
            conf.set("fs.default.name", hdfsUrl);
            fileSystem = FileSystem.get(conf);
            logger.info("get FileSystem by hdfs url:{}, name:{}", hdfsUrl, fileSystem.getUri());
        } else {
            String coreSite = hdfsConfProps.getProperty(SinkerConstants.CORE_SITE);
            String hdfsSite = hdfsConfProps.getProperty(SinkerConstants.HDFS_SITE);
            if (coreSite.startsWith("http")) {
                conf.addResource(new URL(coreSite));
            } else {
                conf.addResource(new Path(coreSite));
            }
            if (hdfsSite.startsWith("http")) {
                conf.addResource(new URL(hdfsSite));
            } else {
                conf.addResource(new Path(hdfsSite));
            }
            conf.setBoolean("fs.hdfs.impl.disable.cache", true);
            fileSystem = FileSystem.newInstance(conf);
            logger.info("get FileSystem by core site :{} ,hdfs site :{},name:{}", coreSite, hdfsSite, fileSystem.getUri());
        }
        this.fileSystem = fileSystem;
    }

    public void close() {
        this.sinkerConfProps = null;
        zkHelper.close();
    }

}
