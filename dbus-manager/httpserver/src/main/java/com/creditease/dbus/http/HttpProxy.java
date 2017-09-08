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
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.mortbay.servlet.ProxyServlet;

/**
 * Created by zhangyf on 17/7/27.
 */
public class HttpProxy extends HttpServer {
    public HttpProxy(int port) throws Exception {
        super(port);
    }

    @Override
    protected void configServer(Server server, int port) {
        ServletHandler handler = new ServletHandler();
        ProxyServlet servlet = new SimpleProxyServlet(ConfUtils.proxyConf);
        ServletHolder servletHolder = new ServletHolder(servlet);

        servletHolder.setAsyncSupported(true);
        servletHolder.setInitParameter("maxThreads", "100");
        handler.addServletWithMapping(servletHolder, "/*");
        server.setHandler(handler);
    }

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(ConfUtils.appConf.getProperty("proxy.server.port"));
        HttpProxy server = new HttpProxy(port);
        Thread t = new Thread(() -> {
            try {
                server.start();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        t.setName("dbus-proxy-server");
        t.start();
        System.out.println("dbus proxy server started on " + port + ".");
    }
}
