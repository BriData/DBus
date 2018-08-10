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

import com.creditease.dbus.domain.model.ProjectTopoTableEncodeOutputColumns;

import java.util.List;

/**
 * User: 王少楠
 * Date: 2018-04-25
 * Time: 下午4:40
 */
public class ProjectTopoTableEncodeOutputColumnsBean {
    private List<ProjectTopoTableEncodeOutputColumns> encodeOutputColumns;
    private int outputListType;

    public List<ProjectTopoTableEncodeOutputColumns> getEncodeOutputColumns() {
        return encodeOutputColumns;
    }

    public void setEncodeOutputColumns(List<ProjectTopoTableEncodeOutputColumns> encodeOutputColumns) {
        this.encodeOutputColumns = encodeOutputColumns;
    }

    public int getOutputListType() {
        return outputListType;
    }

    public void setOutputListType(int outputListType) {
        this.outputListType = outputListType;
    }
}
