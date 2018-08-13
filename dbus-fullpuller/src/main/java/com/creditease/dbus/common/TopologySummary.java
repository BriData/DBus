/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2018 Bridata
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

public class TopologySummary {
    @JsonProperty("id")
    private String topologyId;
    @JsonProperty("name")
    private String topologyName;
    @JsonProperty("status")
    private String status;
    /*@JsonProperty("uptime")
    private String uptime;
    @JsonProperty("uptimeSeconds")
    private String uptimeSeconds;
    @JsonProperty("tasksTotal")
    private String tasksTotal;
    @JsonProperty("workersTotal")
    private String workersTotal;
    @JsonProperty("executorsTotal")
    private String executorsTotal;
    @JsonProperty("replicationCount")
    private String replicationCount;
    @JsonProperty("requestedMemOnHeap")
    private String requestedMemOnHeap;
    @JsonProperty("requestedMemOffHeap")
    private String requestedMemOffHeap;
    @JsonProperty("requestedTotalMem")
    private String requestedTotalMem;
    @JsonProperty("requestedCpu")
    private String requestedCpu;
    @JsonProperty("assignedMemOnHeap")
    private String assignedMemOnHeap;
    @JsonProperty("assignedMemOffHeap")
    private String assignedMemOffHeap;
    @JsonProperty("assignedTotalMem")
    private String assignedTotalMem;
    @JsonProperty("assignedCpu")
    private String assignedCpu;*/
    public String getTopologyId() {
        return topologyId;
    }
    public void setTopologyId(String topologyId) {
        this.topologyId = topologyId;
    }
    public String getTopologyName() {
        return topologyName;
    }
    public void setTopologyName(String topologyName) {
        this.topologyName = topologyName;
    }
    public String getStatus() {
        return status;
    }
    public void setStatus(String status) {
        this.status = status;
    }
/*	public String getUptime() {
        return uptime;
    }
    public void setUptime(String uptime) {
        this.uptime = uptime;
    }
    public String getUptimeSeconds() {
        return uptimeSeconds;
    }
    public void setUptimeSeconds(String uptimeSeconds) {
        this.uptimeSeconds = uptimeSeconds;
    }
    public String getTasksTotal() {
        return tasksTotal;
    }
    public void setTasksTotal(String tasksTotal) {
        this.tasksTotal = tasksTotal;
    }
    public String getWorkersTotal() {
        return workersTotal;
    }
    public void setWorkersTotal(String workersTotal) {
        this.workersTotal = workersTotal;
    }
    public String getExecutorsTotal() {
        return executorsTotal;
    }
    public void setExecutorsTotal(String executorsTotal) {
        this.executorsTotal = executorsTotal;
    }
    public String getReplicationCount() {
        return replicationCount;
    }
    public void setReplicationCount(String replicationCount) {
        this.replicationCount = replicationCount;
    }
    public String getRequestedMemOnHeap() {
        return requestedMemOnHeap;
    }
    public void setRequestedMemOnHeap(String requestedMemOnHeap) {
        this.requestedMemOnHeap = requestedMemOnHeap;
    }
    public String getRequestedMemOffHeap() {
        return requestedMemOffHeap;
    }
    public void setRequestedMemOffHeap(String requestedMemOffHeap) {
        this.requestedMemOffHeap = requestedMemOffHeap;
    }
    public String getRequestedTotalMem() {
        return requestedTotalMem;
    }
    public void setRequestedTotalMem(String requestedTotalMem) {
        this.requestedTotalMem = requestedTotalMem;
    }
    public String getRequestedCpu() {
        return requestedCpu;
    }
    public void setRequestedCpu(String requestedCpu) {
        this.requestedCpu = requestedCpu;
    }
    public String getAssignedMemOnHeap() {
        return assignedMemOnHeap;
    }
    public void setAssignedMemOnHeap(String assignedMemOnHeap) {
        this.assignedMemOnHeap = assignedMemOnHeap;
    }
    public String getAssignedMemOffHeap() {
        return assignedMemOffHeap;
    }
    public void setAssignedMemOffHeap(String assignedMemOffHeap) {
        this.assignedMemOffHeap = assignedMemOffHeap;
    }
    public String getAssignedTotalMem() {
        return assignedTotalMem;
    }
    public void setAssignedTotalMem(String assignedTotalMem) {
        this.assignedTotalMem = assignedTotalMem;
    }
    public String getAssignedCpu() {
        return assignedCpu;
    }
    public void setAssignedCpu(String assignedCpu) {
        this.assignedCpu = assignedCpu;
    }*/

}
