/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2017 Bridata
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

package com.creditease.dbus.common;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MonitorRootInfo {
    @JsonProperty("UpdateTime")
    private String updateTime;
    @JsonProperty("ProducerOffset")
    private String producerOffset;
    @JsonProperty("ConsumerOffset")
    private String consumerOffset;
    @JsonProperty("SplittingTopoStopStatus")
    private String splittingTopoStopStatus;
    @JsonProperty("PullingTopoStopStatus")
    private String pullingTopoStopStatus;

    public String getUpdateTime() {
        return updateTime;
    }
    public void setUpdateTime(String updateTime) {
        this.updateTime = updateTime;
    }
    public String getProducerOffset() {
        return producerOffset;
    }
    public void setProducerOffset(String producerOffset) {
        this.producerOffset = producerOffset;
    }
    public String getConsumerOffset() {
        return consumerOffset;
    }
    public void setConsumerOffset(String consumerOffset) {
        this.consumerOffset = consumerOffset;
    }
    
    public String getSplittingTopoStopStatus() {
        return splittingTopoStopStatus;
    }
    
    public void setSplittingTopoStopStatus(String splittingTopoStopStatus) {
        this.splittingTopoStopStatus = splittingTopoStopStatus;
    }
    
    public String getPullingTopoStopStatus() {
        return pullingTopoStopStatus;
    }
    
    public void setPullingTopoStopStatus(String pullingTopoStopStatus) {
        this.pullingTopoStopStatus = pullingTopoStopStatus;
    }
    
}
