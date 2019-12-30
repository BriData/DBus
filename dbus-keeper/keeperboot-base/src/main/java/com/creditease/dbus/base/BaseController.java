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


package com.creditease.dbus.base;

import com.creditease.dbus.constant.KeeperConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.MessageSource;
import org.springframework.context.MessageSourceAware;
import org.springframework.context.i18n.LocaleContextHolder;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpServletRequest;
import java.util.Locale;

/**
 * Created by zhangyf on 2018/3/17.
 */
public abstract class BaseController implements MessageSourceAware {
    protected Logger logger = LoggerFactory.getLogger(getClass());
    private static ThreadLocal<MessageSource> localMessageSource;

    @Override
    public void setMessageSource(final MessageSource messageSource) {
        if (localMessageSource == null) {
            localMessageSource = new ThreadLocal<MessageSource>() {
                @Override
                protected MessageSource initialValue() {
                    return messageSource;
                }
            };
        }
    }

    protected Locale locale() {
        return LocaleContextHolder.getLocale();
    }

    protected ResultEntityBuilder resultEntityBuilder() {
        MessageSource messageSource = localMessageSource.get();
        return new ResultEntityBuilder(messageSource);
    }

    protected String currentUserRole() {
        HttpServletRequest request = ((ServletRequestAttributes) RequestContextHolder.getRequestAttributes()).getRequest();
        return request.getParameter(KeeperConstants.PARAMETER_KEY_ROLE);
    }

    protected Integer currentUserId() {
        HttpServletRequest request = ((ServletRequestAttributes) RequestContextHolder.getRequestAttributes()).getRequest();
        return Integer.parseInt(request.getParameter(KeeperConstants.PARAMETER_KEY_USER));
    }
}
