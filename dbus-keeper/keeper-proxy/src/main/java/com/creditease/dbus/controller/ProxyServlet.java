package com.creditease.dbus.controller;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Administrator on 2018/8/7.
 */
@WebServlet(name = "proxyServlet", urlPatterns = "/keeper/*", asyncSupported = true)
public class ProxyServlet extends org.eclipse.jetty.proxy.ProxyServlet {

    private Logger logger = LoggerFactory.getLogger(ProxyServlet.class);

    @Override
    protected String rewriteTarget(HttpServletRequest clientRequest) {
        String url = clientRequest.getScheme() + "://" + clientRequest.getServerName() + ":5090/v1" + clientRequest.getRequestURI();
        String queryString = clientRequest.getQueryString();
        if (queryString != null)
            url += "?" + queryString;
        logger.info("url: " + url);
        return url;
    }
}
