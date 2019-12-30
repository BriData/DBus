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


package com.creditease.dbus.allinone.auto.check.resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public abstract class AbstractConfigResource<T> implements IResource<T> {

    protected Logger LOG = LoggerFactory.getLogger(AbstractConfigResource.class);

    protected String name;

    protected Properties prop;

    protected AbstractConfigResource(String name) {
        this.name = name;
    }

    public T load() {
        init();
        return parse();
    }

    public abstract T parse();

    protected abstract void init();

}
