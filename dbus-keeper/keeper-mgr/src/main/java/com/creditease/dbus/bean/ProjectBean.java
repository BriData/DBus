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

import com.creditease.dbus.domain.model.*;

import java.util.List;
import java.util.Map;

/**
 * Created by mal on 2018/3/27.
 */
public class ProjectBean {

    private Project project;

    private List<ProjectUser> users;

    private List<ProjectSink> sinks;

    private List<ProjectResource> resources;

    private Map<Integer, List<ProjectEncodeHint>> encodes;

    public Project getProject() {
        return project;
    }

    public void setProject(Project project) {
        this.project = project;
    }

    public List<ProjectUser> getUsers() {
        return users;
    }

    public void setUsers(List<ProjectUser> users) {
        this.users = users;
    }

    public List<ProjectSink> getSinks() {
        return sinks;
    }

    public void setSinks(List<ProjectSink> sinks) {
        this.sinks = sinks;
    }

    public List<ProjectResource> getResources() {
        return resources;
    }

    public void setResources(List<ProjectResource> resources) {
        this.resources = resources;
    }

    public Map<Integer, List<ProjectEncodeHint>> getEncodes() {
        return encodes;
    }

    public void setEncodes(Map<Integer, List<ProjectEncodeHint>> encodes) {
        this.encodes = encodes;
    }

}
