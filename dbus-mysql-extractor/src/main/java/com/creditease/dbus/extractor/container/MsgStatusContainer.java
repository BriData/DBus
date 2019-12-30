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


package com.creditease.dbus.extractor.container;

import com.creditease.dbus.extractor.common.utils.Constants;
import com.creditease.dbus.extractor.vo.SendStatusVo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class MsgStatusContainer {
    protected Logger logger = LoggerFactory.getLogger(getClass());

    private static MsgStatusContainer msgStatusContainer;

    // 10 mins
    private static final long timeout = 10 * 60 * 1000;

    //key是batchId
    private ConcurrentMap<Long, SendStatusVo> map = new ConcurrentHashMap<Long, SendStatusVo>();

    private MsgStatusContainer() {

    }

    public static MsgStatusContainer getInstance() {
        if (msgStatusContainer == null) {
            synchronized (MsgStatusContainer.class) {
                if (msgStatusContainer == null) {
                    msgStatusContainer = new MsgStatusContainer();
                }
            }
        }
        return msgStatusContainer;
    }

    public int getSize() {
        return map.size();
    }

    public void setTotal(long batchId, int totalSplit, boolean status) {
        SendStatusVo vo = null;
        synchronized (this) {
            vo = map.get(batchId);
            if (vo != null) {
                vo.setTotal(totalSplit);
                vo.setStatus(status);
            } else {
                vo = new SendStatusVo();
                vo.setTotal(totalSplit);
                vo.setStatus(status);
                map.put(batchId, vo);
            }
        }
    }

    // 设置完成数据，不会添加新map项
    public void setCompleted(long batchId, int completed) {
        SendStatusVo vo = null;
        synchronized (this) {
            vo = map.get(batchId);
            if (vo != null) {
                vo.setCompleted(vo.getCompleted() + 1);
            }
        }
    }

    // 设置失败情况，不会添加新map项
    public void setError(long batchId, boolean isErr) {
        SendStatusVo vo = null;
        synchronized (this) {
            vo = map.get(batchId);
            if (vo != null) {
                vo.setError(isErr);
            }
        }
    }

    public void deleteMsg(long batchId) {
        synchronized (this) {
            map.remove(batchId);
        }
    }

    public void clear() {
        synchronized (this) {
            map.clear();
        }
    }

    //从map中复制一份sendstatusvo对象完成状态到treeset中
    //map的对象没有被修改
    // treeset按照batchId进行排序
    public Set<SendStatusVo> getNeedAckOrRollbackBatch() {
        Set<SendStatusVo> setRet = new TreeSet<SendStatusVo>();
        SendStatusVo vo = null;
        Integer numNeedAcl = 0;
        Integer numNeedRollback = 0;
        Integer numNotCompleted = 0;
        for (Map.Entry<Long, SendStatusVo> entry : map.entrySet()) {
            vo = entry.getValue();
            SendStatusVo voRet = new SendStatusVo();
            voRet.setBatchId(entry.getKey());

            //大于1分钟就打印节点状态, 用于调试状态
            //if (System.currentTimeMillis() - vo.getCreateTime() > 60 * 1000) {
            //    logger.info(vo.toString());
            //}

            if (vo.getTotal() != 0 && vo.getCompleted() != 0 && !vo.isError()
                    && vo.isStatus() && (vo.getTotal() <= vo.getCompleted())) {
                voRet.setResult(Constants.NEED_ACK_CANAL);
                numNeedAcl = numNeedAcl + 1;
            } else if ((System.currentTimeMillis() - vo.getCreateTime() > timeout) || vo.isError()) {
                voRet.setResult(Constants.NEED_ROLLBACK_CANAL);
                numNeedRollback = numNeedRollback + 1;
            } else {
                voRet.setResult(Constants.SEND_NOT_COMPLETED);
                numNotCompleted = numNotCompleted + 1;
            }
            setRet.add(voRet);
        }
        logger.debug("up to now, need ack canal number is {}, need rollback canal number is {}, not completed number is {}.",
                numNeedAcl, numNeedRollback, numNotCompleted);
        return setRet;
    }
}
