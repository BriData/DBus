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


/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.creditease.dbus.notopen.mongo;

import com.creditease.dbus.common.format.InputSplit;
import org.bson.Document;

/**
 * A InputSplit of mongo DB.
 */
public class MongoInputSplit extends InputSplit {
    private String pullTargetMongoUrl = null;
    private Document dataQueryObject = null;

    /**
     * Default Constructor.
     */
    public MongoInputSplit() {
    }

    public MongoInputSplit(String pullTargetMongoUrl, Document dataQueryObject) {
        this.pullTargetMongoUrl = pullTargetMongoUrl;
        this.dataQueryObject = dataQueryObject;
    }

    public String getPullTargetMongoUrl() {
        return pullTargetMongoUrl;
    }

    public void setPullTargetMongoUrl(String pullTargetMongoUrl) {
        this.pullTargetMongoUrl = pullTargetMongoUrl;
    }

    public Document getDataQueryObject() {
        return dataQueryObject;
    }

    public void setDataQueryObject(Document dataQueryObject) {
        this.dataQueryObject = dataQueryObject;
    }

    public long getLength() {
        return -1;
    }

    @Override
    public String toString() {
        return "MongoInputSplit{" +
                "pullTargetMongoUrl='" + pullTargetMongoUrl + '\'' +
                ", dataQueryObject=" + dataQueryObject +
                '}';
    }
}
