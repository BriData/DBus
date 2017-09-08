/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2017 Bridata
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

package com.creditease.dbus.http;

import com.creditease.dbus.mgr.base.ConfUtils;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Configurator;
import org.eclipse.jetty.server.Server;

import java.io.FileInputStream;
import java.io.InputStream;

/**
 * Created by zhangyf on 17/8/1.
 */
public abstract class HttpServer {
    private Server server;

    static {
        InputStream is = null;
        try {
            is = new FileInputStream(ConfUtils.LOG4J_XML);
            ConfigurationSource source = new ConfigurationSource(is);
            Configurator.initialize(HttpProxy.class.getClassLoader(), source);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (is != null) is.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public HttpServer(int port) throws Exception {
        createServer(port);
    }

    public void start() throws Exception {
        server.start();
    }

    public void stop() throws Exception {
        server.stop();
    }

    protected void createServer(int port) {
        server = new Server(port);
        configServer(server, port);
    }

    protected abstract void configServer(Server server, int port);
}
