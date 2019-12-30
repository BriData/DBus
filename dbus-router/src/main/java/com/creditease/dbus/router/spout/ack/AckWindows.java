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


package com.creditease.dbus.router.spout.ack;

import com.creditease.dbus.router.bean.Ack;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by Administrator on 2018/6/4.
 */
public class AckWindows {

    private Logger logger = LoggerFactory.getLogger(AckWindows.class);

    private Map<String, Map<Long, Ack>> ackBooks = new HashMap<>();

    private long time = 0;

    private long baseTime = 0;

    private AckCallBack callBack = null;

    public AckWindows(long time, AckCallBack cb) {
        this.time = time;
        this.baseTime = System.currentTimeMillis();
        this.callBack = cb;
    }

    public String obtainKey(Ack ackVo) {
        return StringUtils.joinWith("_", ackVo.getTopic(), ackVo.getPartition());
    }

    public void add(Ack ackVo) {
        ackVo.setStatus(Ack.INIT);
        String key = obtainKey(ackVo);
        if (ackBooks.containsKey(key)) {
            ackBooks.get(key).put(ackVo.getOffset(), ackVo);
        } else {
            Map<Long, Ack> map = new TreeMap<>();
            map.put(ackVo.getOffset(), ackVo);
            ackBooks.put(key, map);
        }
    }

    public void ack(Ack ackVo) {
        logger.debug("topic:{}, partaiton:{}, offset:{}, trigger ack.", ackVo.getTopic(), ackVo.getPartition(), ackVo.getOffset());
        doAckOrFail(ackVo, Ack.OK);
        flush();
    }

    public void fail(Ack ackVo) {
        logger.debug("topic:{}, partaiton:{}, offset:{}, trigger fail.", ackVo.getTopic(), ackVo.getPartition(), ackVo.getOffset());
        if (StringUtils.endsWith(ackVo.getTopic(), "_ctrl"))
            ackBooks.get(obtainKey(ackVo)).get(ackVo.getOffset()).setStatus(Ack.OK);
        else doAckOrFail(ackVo, Ack.FAIL);
        flush();
    }

    public int size() {
        int size = 0;
        for (String key : ackBooks.keySet())
            size += ackBooks.get(key).size();
        return size;
    }

    private void doAckOrFail(Ack ackVo, int status) {
        Map<Long, Ack> book = ackBooks.get(obtainKey(ackVo));
        if (book == null || book.isEmpty()) {
            logger.debug("topic:{}, partaiton:{}, offset:{}, 发生过fail,已将队列清空,执行跳过处理.",
                    ackVo.getTopic(), ackVo.getPartition(), ackVo.getOffset());
            return;
        }
        if (book.get(ackVo.getOffset()) == null) {
            logger.debug("topic:{}, partaiton:{}, offset:{}, 发生过fail,已将该ack对象清空,执行跳过处理.",
                    ackVo.getTopic(), ackVo.getPartition(), ackVo.getOffset());
            return;
        }
        book.get(ackVo.getOffset()).setStatus(status);
    }

    public void flush() {
        if (System.currentTimeMillis() - baseTime >= time) {
            for (String key : ackBooks.keySet()) {
                Map<Long, Ack> map = ackBooks.get(key);
                Ack preValue = null;
                boolean isFail = false;
                List<Long> okOffsetList = new ArrayList<>();
                for (Map.Entry<Long, Ack> entry : map.entrySet()) {
                    Ack value = entry.getValue();
                    if (Ack.INIT == value.getStatus()) {
                        logger.info("topic:{}, partaiton:{}, offset:{}, status: init",
                                value.getTopic(), value.getPartition(), value.getOffset());
                        if (preValue != null) this.callBack.ack(preValue);
                        break;
                    } else if (Ack.OK == value.getStatus()) {
                        logger.info("topic:{}, partaiton:{}, offset:{}, status: ok",
                                value.getTopic(), value.getPartition(), value.getOffset());
                        okOffsetList.add(value.getOffset());
                        preValue = value;
                        continue;
                    } else if (Ack.FAIL == value.getStatus()) {
                        logger.info("topic:{}, partaiton:{}, offset:{}, status: fail",
                                value.getTopic(), value.getPartition(), value.getOffset());
                        this.callBack.fail(value);
                        isFail = true;
                        break;
                    }
                }
                if (isFail) {
                    ackBooks.get(key).clear();
                } else {
                    for (Long offset : okOffsetList) {
                        ackBooks.get(key).remove(offset);
                    }
                }
            }
            baseTime = System.currentTimeMillis();
        }
    }

    public void clear() {
        flush();
        for (String key : ackBooks.keySet()) ackBooks.get(key).clear();
    }
}
