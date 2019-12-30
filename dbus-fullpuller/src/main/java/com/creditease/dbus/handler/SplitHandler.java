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

import com.creditease.dbus.common.bean.DBConfiguration;
import com.creditease.dbus.common.bean.ProgressInfo;
import com.creditease.dbus.common.bean.ProgressInfoParam;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.ZkService;
import com.creditease.dbus.helper.FullPullHelper;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public abstract class SplitHandler {
    private Logger logger = LoggerFactory.getLogger(getClass());

    protected DBConfiguration dbConf;
    protected String reqString;
    protected ZkService zkService;
    protected Properties commonProps;
    protected Producer byteProducer;
    protected String dataType;

    public abstract void executeSplit() throws Exception;

    public void setDbConf(DBConfiguration dbConf) {
        this.dbConf = dbConf;
    }

    public void setReqString(String reqString) {
        this.reqString = reqString;
    }

    public void setZkService(ZkService zkService) {
        this.zkService = zkService;
    }

    public void setCommonProps(Properties commonProps) {
        this.commonProps = commonProps;
    }

    public void setByteProducer(Producer byteProducer) {
        this.byteProducer = byteProducer;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    protected void writeSplitResultToZkAndDB(Long totalPartition, Long totalShardsCount, Long totalRowsCount, Long firstShardOffset,
                                             Long lastShardOffset) throws Exception {
        ProgressInfoParam infoParam = new ProgressInfoParam();
        infoParam.setTotalCount(totalShardsCount);
        infoParam.setPartitions(totalPartition);
        infoParam.setTotalRows(totalRowsCount);
        infoParam.setSplitStatus(Constants.FULL_PULL_STATUS_ENDING);
        ProgressInfo progressInfo = FullPullHelper.updateZkNodeInfoWithVersion(zkService, FullPullHelper.getMonitorNodePath(reqString), infoParam);
        FullPullHelper.updateStatusToFullPullHistoryTable(progressInfo, FullPullHelper.getSeqNo(reqString), null, firstShardOffset, lastShardOffset);
    }

    protected void writerSplitStatusToZkAndDB(String progressInfoNodePath, Long totalPartition, Long totalShardsCount, Long totalRows) throws Exception {
        ProgressInfoParam infoParam = new ProgressInfoParam();
        infoParam.setTotalCount(totalShardsCount);
        infoParam.setPartitions(totalPartition);
        infoParam.setTotalRows(totalRows);
        ProgressInfo progressInfo = FullPullHelper.updateZkNodeInfoWithVersion(zkService, progressInfoNodePath, infoParam);
        FullPullHelper.updateStatusToFullPullHistoryTable(progressInfo, FullPullHelper.getSeqNo(reqString), null, null, null);
    }

}
