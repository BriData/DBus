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


package com.creditease.dbus.service;

import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.base.ResultEntityBuilder;
import com.creditease.dbus.base.com.creditease.dbus.utils.RequestSender;
import com.creditease.dbus.constant.ServiceNames;
import com.creditease.dbus.domain.model.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.MessageSource;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

/**
 * Created by zhangyf on 2018/3/12.
 */
@Service
public class AuthenticationService {
    private Logger logger = LoggerFactory.getLogger(getClass());
    @Autowired
    private RequestSender sender;
    @Autowired
    private MessageSource messageSource;

    public ResultEntity doLogin(String email, String password) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/users/query-by-email", "email={0}", email);
        ResultEntity entity = result.getBody();
        if (entity.success()) {
            User user = entity.getPayload(User.class);
            ResultEntityBuilder rb = new ResultEntityBuilder(messageSource);
            if (user != null && password.equals(user.getPassword())) {
                return rb.payload(user).build();
            } else {
                return rb.status(10003).build();
            }
        }
        return entity;
    }

    public String md5(String pwd) {
        return pwd;
    }
}
