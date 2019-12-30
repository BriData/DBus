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


package com.creditease.dbus.common;

import com.alibaba.fastjson.JSONObject;

/**
 * This is Description
 *
 * @author xiancangao
 * @date 2018/12/31
 */
public class MultiTenancyHelper {

    public static boolean isMultiTenancy(String reqString) {
        JSONObject reqJson = JSONObject.parseObject(reqString);
        JSONObject projectJson = reqJson.getJSONObject(FullPullConstants.REQ_PROJECT);
        if (projectJson != null && !projectJson.isEmpty()) {
            return true;
        }
        return false;
    }

    public static boolean isMultiTenancy(JSONObject reqJson) {
        JSONObject projectJson = reqJson.getJSONObject(FullPullConstants.REQ_PROJECT);
        if (projectJson != null && !projectJson.isEmpty()) {
            return true;
        }
        return false;
    }


}
