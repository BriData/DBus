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


package com.creditease.dbus.ogg.container;

import com.creditease.dbus.ogg.bean.ExtractConfigBean;

/**
 * User: 王少楠
 * Date: 2018-08-28
 * Desc:
 */
public class ExtractConfigContainer {
    private ExtractConfigBean extrConfig;

    private static ExtractConfigContainer container;

    private ExtractConfigContainer() {
    }

    public static ExtractConfigContainer getInstance() {
        if (container == null) {
            synchronized (ExtractConfigContainer.class) {
                if (container == null)
                    container = new ExtractConfigContainer();
            }
        }
        return container;
    }

    public ExtractConfigBean getExtrConfig() {
        return extrConfig;
    }

    public void setExtrConfig(ExtractConfigBean extrConfig) {
        this.extrConfig = extrConfig;
    }
}
