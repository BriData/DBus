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

import com.creditease.dbus.bean.HdfsOutputStreamInfo;
import com.creditease.dbus.cache.LocalCache;
import com.creditease.dbus.commons.DBusConsumerRecord;
import com.creditease.dbus.helper.SinkerHelper;
import com.creditease.dbus.tools.SinkerBaseMap;
import com.creditease.dbus.tools.SinkerConstants;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SinkerHdfsWritehandler implements SinkerWriteHandler {
    private Logger logger = LoggerFactory.getLogger(getClass());
    private SinkerBaseMap sinkerConf;

    @Override
    public void sendData(SinkerBaseMap sinkerConf, DBusConsumerRecord<String, byte[]> data, String value) throws Exception {
        HdfsOutputStreamInfo outPutStream = null;
        try {
            this.sinkerConf = sinkerConf;
            outPutStream = getOutPutStream(data, value);
            FSDataOutputStream outputStream = outPutStream.getOutputStream();
            outputStream.write(value.getBytes("utf-8"));
            outPutStream.hsync();
        } catch (Exception e) {
            logger.error("[write bolt] send data to hdfs error.", e);
            String filePath = outPutStream.getFilePath();
            try {
                String[] dataKeys = StringUtils.split(data.key(), ".");
                String key = String.format("%s.%s.%s", dataKeys[2], dataKeys[3], dataKeys[4]);
                LocalCache.remove(key);
                if (outPutStream != null) {
                    outPutStream.clear();
                }
                logger.info("[write bolt] close error outStream use filePath {}", filePath);
            } catch (Exception e1) {
                logger.error("close FSDataOutputStream error ,{}", filePath, e1);
                throw e1;
            }
            throw e;
        }
    }

    private HdfsOutputStreamInfo getOutPutStream(DBusConsumerRecord<String, byte[]> data, String value) throws Exception {
        String[] dataKeys = StringUtils.split(data.key(), ".");
        String version = dataKeys[5];
        String key = String.format("%s.%s.%s", dataKeys[2], dataKeys[3], dataKeys[4]);
        HdfsOutputStreamInfo hdfsOutputStreamInfo = LocalCache.get(key);
        if (hdfsOutputStreamInfo != null) {
            Long hdfsFileSize = Long.parseLong(sinkerConf.hdfsConfProps.getProperty(SinkerConstants.HDFS_FILE_SIZE));
            FSDataOutputStream outputStream = hdfsOutputStreamInfo.getOutputStream();
            //超过hdfs文件块大小,需要切换文件
            if (((outputStream.getPos() + value.length()) > hdfsFileSize || SinkerHelper.needCreateNewFile(version, hdfsOutputStreamInfo.getFilePath()))) {
                logger.info("[write bolt] will close outStream use filePath {}", hdfsOutputStreamInfo.getFilePath());
                hdfsOutputStreamInfo.clear();
                hdfsOutputStreamInfo = null;
            }
        }
        if (hdfsOutputStreamInfo == null) {
            String hdfsRootPath = sinkerConf.hdfsConfProps.getProperty(SinkerConstants.HDFS_ROOT_PATH);
            String fileName = SinkerHelper.getHdfsFileName();
            String path = SinkerHelper.getHdfsFilePath(dataKeys, hdfsRootPath, fileName);
            logger.info("[write bolt] will create outStream use filePath {}", path);
            FSDataOutputStream outputStream = sinkerConf.fileSystem.create(new Path(path));
            hdfsOutputStreamInfo = new HdfsOutputStreamInfo(fileName, path, outputStream, version, sinkerConf.hsyncIntervals);
            LocalCache.put(key, hdfsOutputStreamInfo);
        }
        return hdfsOutputStreamInfo;
    }
}
