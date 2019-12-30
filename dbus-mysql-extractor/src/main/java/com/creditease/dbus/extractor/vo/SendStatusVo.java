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


package com.creditease.dbus.extractor.vo;

import com.creditease.dbus.extractor.common.utils.Constants;

public class SendStatusVo implements Comparable {
    private long batchId;
    private int total;            //同一个batchId的数据拆分发给kafka的总片数
    private int completed;        //成功完成的片数
    private boolean status;    //true表示被拆分的批次已经放入队列，false是表示拆分填充还没有结束呢
    private boolean isError;    //默认为没有错误，一旦设置错误，整个批次都失败
    private int result;        //1:需要给canal ack； 2:需要向canal rollback；3:未完成
    private long createTime;

    public SendStatusVo() {
        createTime = System.currentTimeMillis();
        total = 0;
        completed = 0;
        status = false;
        isError = false;
        result = Constants.SEND_NOT_COMPLETED;
    }

    public long getBatchId() {
        return batchId;
    }

    public void setBatchId(long batchId) {
        this.batchId = batchId;
    }

    public int getTotal() {
        return total;
    }

    public void setTotal(int total) {
        this.total = total;
    }

    public int getCompleted() {
        return completed;
    }

    public void setCompleted(int complete) {
        this.completed = complete;
    }

    public boolean isStatus() {
        return status;
    }

    public void setStatus(boolean status) {
        this.status = status;
    }

    public boolean isError() {
        return isError;
    }

    public void setError(boolean isError) {
        this.isError = isError;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public int getResult() {
        return result;
    }

    public void setResult(int result) {
        this.result = result;
    }

    public int compareTo(Object o) {
        SendStatusVo vo = (SendStatusVo) o;
        return (int) (this.batchId - vo.batchId); //>0:升序；<0：降序
    }

    @Override
    public String toString() {
        return "SendStatusVo{" +
                "batchId=" + batchId +
                ", total=" + total +
                ", completed=" + completed +
                ", status=" + status +
                ", isError=" + isError +
                ", result=" + result +
                ", createTime=" + createTime +
                '}';
    }
}
