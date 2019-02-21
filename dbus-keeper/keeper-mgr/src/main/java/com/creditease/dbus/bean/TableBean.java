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

package com.creditease.dbus.bean;
import com.creditease.dbus.domain.model.ProjectTopoTable;

import java.util.List;
import java.util.Map;

/**
 * User: 王少楠
 * Date: 2018-04-25
 * Time: 下午3:43
 */
public class TableBean {

    //topoTableId
    private Integer Id;

    private Integer projectId;

    private Integer topoId;

    private String outputTopic;

    private String outputType;

    private Integer sinkId;

    Map<Integer,ProjectTopoTableEncodeOutputColumnsBean> encodes;

    public Integer getId() {
        return Id;
    }

    public void setId(Integer id) {
        Id = id;
    }

    public Integer getProjectId() {
        return projectId;
    }

    public void setProjectId(Integer projectId) {
        this.projectId = projectId;
    }

    public Integer getTopoId() {
        return topoId;
    }

    public void setTopoId(Integer topoId) {
        this.topoId = topoId;
    }

    public String getOutputTopic() {
        return outputTopic;
    }

    public void setOutputTopic(String outputTopic) {
        this.outputTopic = outputTopic;
    }

    public String getOutputType() {
        return outputType;
    }

    public void setOutputType(String outputType) {
        this.outputType = outputType;
    }

    public Integer getSinkId() {
        return sinkId;
    }

    public void setSinkId(Integer sinkId) {
        this.sinkId = sinkId;
    }

    public Map<Integer, ProjectTopoTableEncodeOutputColumnsBean> getEncodes() {
        return encodes;
    }

    public void setEncodes(Map<Integer, ProjectTopoTableEncodeOutputColumnsBean> encodes) {
        this.encodes = encodes;
    }

}
