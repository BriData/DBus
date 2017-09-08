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

package com.creditease.dbus.ws;

import com.creditease.dbus.mgr.base.ConfUtils;
import com.creditease.dbus.mgr.base.LogInitializator;
import org.eclipse.jetty.server.Server;
import org.glassfish.jersey.jetty.JettyHttpContainerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import javax.ws.rs.core.UriBuilder;
import java.net.URI;

public class HttpServerLauncher extends LogInitializator {
    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(ConfUtils.appConf.getProperty("rest.server.port"));
        URI baseUri = UriBuilder.fromUri("http://localhost").port(port).build();
        ResourceConfig config = new RestApplication();
        Server server = JettyHttpContainerFactory.createServer(baseUri, config, false);
        server.start();
    }
}
