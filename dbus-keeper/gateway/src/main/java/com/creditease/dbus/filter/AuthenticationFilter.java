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


package com.creditease.dbus.filter;

import com.creditease.dbus.base.ResultEntityBuilder;
import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.utils.JWTUtils;
import com.netflix.zuul.ZuulFilter;
import com.netflix.zuul.context.RequestContext;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.Jws;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.cloud.netflix.zuul.filters.support.FilterConstants;
import org.springframework.context.MessageSource;
import org.springframework.http.MediaType;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;

import static com.creditease.dbus.constant.KeeperConstants.*;

/**
 * Created by zhangyf on 2018/3/8.
 */
@RefreshScope
public class AuthenticationFilter extends ZuulFilter {
    private static Logger logger = LoggerFactory.getLogger(AuthenticationFilter.class);

    @Value("${token.sign.key}")
    private String SIGNING_KEY;

    @Value("${token.expire.minutes}")
    private int tokenExpireMinutes;

    @Autowired
    private MessageSource ms;

    @Override
    public String filterType() {
        return FilterConstants.PRE_TYPE;
    }

    @Override
    public int filterOrder() {
        return 0;
    }

    @Override
    public boolean shouldFilter() {
        RequestContext ctx = RequestContext.getCurrentContext();
        HttpServletRequest request = ctx.getRequest();
        return !request.getRequestURI().endsWith("/users/create")
                && !request.getRequestURI().endsWith("/configCenter/isInitialized")
                && !request.getRequestURI().endsWith("/configCenter/getBasicConf")
                && !request.getRequestURI().endsWith("/configCenter/updateBasicConf")
                && !request.getRequestURI().endsWith("/accessDbusPreTreated/check/1")
                && !request.getRequestURI().endsWith("/accessDbusPreTreated/downloadExcleModel");

    }

    @Override
    public Object run() {
        RequestContext ctx = RequestContext.getCurrentContext();
        HttpServletRequest request = ctx.getRequest();
        logger.info(String.format("%s request to %s", request.getMethod(), request.getRequestURL().toString()));

        String token = getToken(request);

        ResultEntityBuilder builder = new ResultEntityBuilder(ms);
        if (StringUtils.isNotBlank(token)) {
            try {
                Jws<Claims> jws = JWTUtils.parseToken(token, SIGNING_KEY);

                setParameter(ctx, PARAMETER_KEY_USER, jws.getBody().get("userId").toString());
                setParameter(ctx, PARAMETER_KEY_EMAIL, jws.getBody().get("email").toString());
                setParameter(ctx, PARAMETER_KEY_ROLE, jws.getBody().get("roleType").toString());

                logger.info("{}服务收到请求,URL参数:{},表单参数:{}", request.getRequestURI(), ctx.getRequestQueryParams(), getRequestBody(request));
                ctx.setSendZuulResponse(true);
                return null;
            } catch (ExpiredJwtException ee) {
                ctx.setResponseBody(builder.status(MessageCode.TOKEN_EXPIRED).build().toJSON());
            } catch (Exception e) {
                logger.error("parse token error", e);
                ctx.setResponseBody(builder.status(MessageCode.TOKEN_ILLEGAL).build().toJSON());
            }
        } else {
            ctx.setResponseBody(builder.status(MessageCode.TOKEN_NOT_FOUND).build().toJSON());
        }
        ctx.getResponse().setContentType(MediaType.APPLICATION_JSON_UTF8.toString());
        ctx.setSendZuulResponse(false);
        return null;
    }

    private String getRequestBody(HttpServletRequest request) {
        BufferedReader br = null;
        try {
            if (request.getContentType() != null && request.getContentType().contains("application/json")) {
                br = new BufferedReader(new InputStreamReader(request.getInputStream(), "utf-8"));
                StringBuffer sb = new StringBuffer("");
                String temp;
                while ((temp = br.readLine()) != null) {
                    sb.append(temp);
                }
                return sb.toString();
            }
            return null;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return null;
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    /**
     * order：1 cookie 2 header.Authorization 3 request parameter
     *
     * @return token string
     */
    private String getToken(HttpServletRequest request) {
        String token = request.getParameter("token");
        if (StringUtils.isBlank(token)) {
            Cookie[] cookies = request.getCookies();
            if (cookies != null) {
                for (Cookie cookie : cookies) {
                    if (cookie.getName().equalsIgnoreCase("token")) {
                        token = cookie.getValue();
                        break;
                    }
                }
            }
        }
        if (StringUtils.isBlank(token)) {
            token = request.getHeader("Authorization");
        }


        return token;
    }

    public void setParameter(RequestContext ctx, String key, String parameter) {
        if (ctx.getRequestQueryParams() == null) ctx.setRequestQueryParams(new HashMap<>());
        ctx.getRequestQueryParams().put(key, Arrays.asList(parameter));
    }
}
