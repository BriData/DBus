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


package com.creditease.dbus.ogg.resource;

import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * User: 王少楠
 * Date: 2018-08-24
 * Desc:
 */
public abstract class AbstractConfigResource<T> implements IResource<T> {

    protected String name;

    protected Properties props;

    protected AbstractConfigResource(String name) {
        this.name = name;
    }

    public T load() throws Exception {
        init();
        return parse();
    }

    public abstract T parse();

    protected void init() throws Exception {
        FileInputStream fis = null;
        try {
            props = new Properties();
            File file = new File(System.getProperty("user.dir") + "/conf/" + name);
            fis = new FileInputStream(file);
            props.load(fis);
        } catch (IOException e) {
            throw e;
        } finally {
            IOUtils.closeQuietly(fis);
        }
    }
}
