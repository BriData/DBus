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


package com.creditease.dbus.commons;

import java.io.Serializable;

public class FullPullNodeVo implements Serializable {

    /**
     * serialVersionUID
     */
    private static final long serialVersionUID = -696458928348997426L;

    private String UpdateTime;

    private Long ProducerOffset;

    private Long ConsumerOffset;

    public String getUpdateTime() {
        return UpdateTime;
    }

    public void setUpdateTime(String updateTime) {
        UpdateTime = updateTime;
    }

    public Long getProducerOffset() {
        return ProducerOffset;
    }

    public void setProducerOffset(Long producerOffset) {
        ProducerOffset = producerOffset;
    }

    public Long getConsumerOffset() {
        return ConsumerOffset;
    }

    public void setConsumerOffset(Long consumerOffset) {
        ConsumerOffset = consumerOffset;
    }

}
