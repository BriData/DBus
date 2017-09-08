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
import org.apache.commons.lang3.SystemUtils;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Created by zhangyf on 17/8/1.
 */
public class ResourceHttpServer extends HttpServer {
    private static Logger logger = LoggerFactory.getLogger(ResourceHttpServer.class);

    public ResourceHttpServer(int port) throws Exception {
        super(port);
    }

    @Override
    protected void configServer(Server server, int port) {
        ServerConnector connector = new ServerConnector(server);
        connector.setPort(port);
        server.setConnectors(new Connector[]{connector});

        ResourceHandler handler = new ResourceHandler();
        ContextHandler context = new ContextHandler();
        context.setContextPath("/dbus/");

        context.setResourceBase(SystemUtils.USER_DIR + File.separator + "html");
        context.setHandler(handler);

        server.setHandler(context);
    }

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(ConfUtils.appConf.getProperty("http.server.port"));
        HttpServer server = new ResourceHttpServer(port);
        Thread t = new Thread(() -> {
            try {
                server.start();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        t.setName("dbus-http-server");
        t.start();
        logger.info("dbus http server started on {}.", port + "");
    }
}
