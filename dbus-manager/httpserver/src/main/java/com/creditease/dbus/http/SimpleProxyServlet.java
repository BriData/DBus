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
import org.mortbay.servlet.ProxyServlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by zhangyf on 17/7/31.
 */
public class SimpleProxyServlet extends ProxyServlet {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private static Map<String, String> cache = new HashMap<>();

    public SimpleProxyServlet(Properties proxyProps) {
        cache.put("/mgr/", buildURL("/mgr/", proxyProps, "manager.server.port"));
        cache.put("/dbus/", buildURL("/dbus/", proxyProps, "http.server.port"));
        cache.put("/webservice/", buildURL("/webservice/", proxyProps, "rest.server.port"));
    }

    private String buildURL(String cacheKey, Properties props, String key) {
        return props.getProperty(cacheKey).replace("${port}", ConfUtils.appConf.getProperty(key));
    }

    @Override
    protected URL proxyHttpURL(String scheme, String serverName, int serverPort, String uri) throws MalformedURLException {
        return redirectURL(uri);
    }

    private URL redirectURL(String url) throws MalformedURLException {
        for (String prefix : cache.keySet()) {
            if (url.startsWith(prefix)) {
                String path = url.replace(prefix, "");
                String urlStr = cache.get(prefix);
                if (urlStr.endsWith("/")) urlStr = urlStr.substring(0, urlStr.length() - 1);
                if (path.startsWith("/")) path = path.substring(1, path.length());
                URL target = new URL(urlStr + "/" + path);
                logger.info("proxy: original-{}, target-{}", url, target.toString());
                return target;
            }
        }
        logger.warn("proxy: original-{}, no target found.", url);
        return null;
    }
}
