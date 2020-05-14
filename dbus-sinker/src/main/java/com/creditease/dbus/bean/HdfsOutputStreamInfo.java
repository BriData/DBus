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


package com.creditease.dbus.bean;

import org.apache.hadoop.fs.FSDataOutputStream;

public class HdfsOutputStreamInfo {
    private String fileName;
    private String version;
    private String filePath;
    private FSDataOutputStream outputStream;
    private Integer hsyncIntervals;
    private Long lastHsyncTime;

    public HdfsOutputStreamInfo(String fileName, String filePath, FSDataOutputStream outputStream, String version, Integer hsyncIntervals) {
        this.fileName = fileName;
        this.filePath = filePath;
        this.outputStream = outputStream;
        this.version = version;
        this.hsyncIntervals = hsyncIntervals;
        this.lastHsyncTime = System.currentTimeMillis();
    }

    public void clear() throws Exception {
        this.version = null;
        this.filePath = null;
        if (this.outputStream != null) {
            this.outputStream.close();
        }
        this.outputStream = null;
    }

    public void closeOs() throws Exception {
        if (this.outputStream != null) {
            this.outputStream.close();
        }
        this.outputStream = null;
    }

    public boolean close() throws Exception {
        // 间隔一定时间关闭一次,暂时沿用hync的间隔配置
        if ((System.currentTimeMillis() - lastHsyncTime) > hsyncIntervals) {
            if (this.outputStream != null) {
                this.outputStream.close();
            }
            this.lastHsyncTime = System.currentTimeMillis();
            this.outputStream = null;
            return true;
        }
        return false;
    }

    public void hsync() throws Exception {
        outputStream.hsync();
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public FSDataOutputStream getOutputStream() {
        return outputStream;
    }

    public void setOutputStream(FSDataOutputStream outputStream) {
        this.outputStream = outputStream;
    }

}
