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


package com.creditease.dbus.router.spout.context;

import java.util.List;

import com.creditease.dbus.router.base.DBusRouterBase;
import com.creditease.dbus.router.bean.Sink;
import com.creditease.dbus.router.spout.BaseSpout;

/**
 * Created by Administrator on 2018/7/9.
 */
public class ProcessorContext extends Context {

    private BaseSpout spout = null;
    private List<Sink> sinks = null;

    public ProcessorContext(BaseSpout spout, DBusRouterBase inner, List<Sink> sinks) {
        super(inner);
        this.spout = spout;
        this.sinks = sinks;
    }

    public BaseSpout getSpout() {
        return spout;
    }

    public void setSpout(BaseSpout spout) {
        this.spout = spout;
    }

    public List<Sink> getSinks() {
        return sinks;
    }

    public void setSinks(List<Sink> sinks) {
        this.sinks = sinks;
    }

}
