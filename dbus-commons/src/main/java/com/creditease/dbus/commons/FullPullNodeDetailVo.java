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

public class FullPullNodeDetailVo implements Serializable {

    /**
     * serialVersionUID
     */
    private static final long serialVersionUID = -696458928348997426L;

    private String UpdateTime;

    private String CreateTime;

    private String StartTime;

    private String EndTime;

    private String ErrorMsg;

    private Long TotalCount;

    private Long FinishedCount;

    private Integer Partitions;

    private String TotalRows;

    private String FinishedRows;

    private String StartSecs;

    private String ConsumeSecs;

    private String Status;

    public String getUpdateTime() {
        return UpdateTime;
    }

    public void setUpdateTime(String updateTime) {
        UpdateTime = updateTime;
    }

    public String getCreateTime() {
        return CreateTime;
    }

    public void setCreateTime(String createTime) {
        CreateTime = createTime;
    }

    public String getStartTime() {
        return StartTime;
    }

    public void setStartTime(String startTime) {
        StartTime = startTime;
    }

    public String getEndTime() {
        return EndTime;
    }

    public void setEndTime(String endTime) {
        EndTime = endTime;
    }

    public String getErrorMsg() {
        return ErrorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        ErrorMsg = errorMsg;
    }

    public Long getTotalCount() {
        return TotalCount;
    }

    public void setTotalCount(Long totalCount) {
        TotalCount = totalCount;
    }

    public Long getFinishedCount() {
        return FinishedCount;
    }

    public void setFinishedCount(Long finishedCount) {
        FinishedCount = finishedCount;
    }

    public Integer getPartitions() {
        return Partitions;
    }

    public void setPartitions(Integer partitions) {
        Partitions = partitions;
    }

    public String getTotalRows() {
        return TotalRows;
    }

    public void setTotalRows(String totalRows) {
        TotalRows = totalRows;
    }

    public String getFinishedRows() {
        return FinishedRows;
    }

    public void setFinishedRows(String finishedRows) {
        FinishedRows = finishedRows;
    }

    public String getStartSecs() {
        return StartSecs;
    }

    public void setStartSecs(String startSecs) {
        StartSecs = startSecs;
    }

    public String getConsumeSecs() {
        return ConsumeSecs;
    }

    public void setConsumeSecs(String consumeSecs) {
        ConsumeSecs = consumeSecs;
    }

    public String getStatus() {
        return Status;
    }

    public void setStatus(String status) {
        Status = status;
    }

    public String toHtml() {
        StringBuilder html = new StringBuilder();
        html.append("<table bgcolor=\"#c1c1c1\">");
        html.append("<tr bgcolor=\"#ffffff\">");
        html.append("    <th align=\"left\">Partitions</th>");
        html.append("    <th align=\"left\">" + getPartitions() + "</th>");
        html.append("</tr>");
        html.append("<tr bgcolor=\"#ffffff\">");
        html.append("    <th align=\"left\">TotalCount</th>");
        html.append("    <th align=\"left\">" + getTotalCount() + "</th>");
        html.append("</tr>");
        html.append("<tr bgcolor=\"#ffffff\">");
        html.append("    <th align=\"left\">FinishedCount</th>");
        html.append("    <th align=\"left\">" + getFinishedCount() + "</th>");
        html.append("</tr>");
        html.append("<tr bgcolor=\"#ffffff\">");
        html.append("    <th align=\"left\">TotalRows</th>");
        html.append("    <th align=\"left\">" + getTotalRows() + "</th>");
        html.append("</tr>");
        html.append("<tr bgcolor=\"#ffffff\">");
        html.append("    <th align=\"left\">FinishedRows</th>");
        html.append("    <th align=\"left\">" + getFinishedRows() + "</th>");
        html.append("</tr>");
        html.append("<tr bgcolor=\"#ffffff\">");
        html.append("    <th align=\"left\">StartSecs</th>");
        html.append("    <th align=\"left\">" + getStartSecs() + "</th>");
        html.append("</tr>");
        html.append("<tr bgcolor=\"#ffffff\">");
        html.append("    <th align=\"left\">ConsumeSecs</th>");
        html.append("    <th align=\"left\">" + getConsumeSecs() + "</th>");
        html.append("</tr>");
        html.append("<tr bgcolor=\"#ffffff\">");
        html.append("    <th align=\"left\">CreateTime</th>");
        html.append("    <th align=\"left\">" + getCreateTime() + "</th>");
        html.append("</tr>");
        html.append("<tr bgcolor=\"#ffffff\">");
        html.append("    <th align=\"left\">UpdateTime</th>");
        html.append("    <th align=\"left\">" + getUpdateTime() + "</th>");
        html.append("</tr>");
        html.append("<tr bgcolor=\"#ffffff\">");
        html.append("    <th align=\"left\">StartTime</th>");
        html.append("    <th align=\"left\">" + getStartTime() + "</th>");
        html.append("</tr>");
        html.append("<tr bgcolor=\"#ffffff\">");
        html.append("    <th align=\"left\">EndTime</th>");
        html.append("    <th align=\"left\">" + getEndTime() + "</th>");
        html.append("</tr>");
        html.append("<tr bgcolor=\"#ffffff\">");
        html.append("    <th align=\"left\">Status</th>");
        html.append("    <th align=\"left\">" + getStatus() + "</th>");
        html.append("</tr>");
        html.append("<tr bgcolor=\"#ffffff\">");
        html.append("    <th align=\"left\">ErrorMsg</th>");
        html.append("    <th align=\"left\">" + getErrorMsg() + "</th>");
        html.append("</tr>");
        html.append("</table>");
        return html.toString();
    }

}
