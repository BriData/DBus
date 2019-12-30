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

import com.creditease.dbus.domain.model.User;
import org.apache.commons.beanutils.BeanUtilsBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.MessageSource;
import org.springframework.context.i18n.LocaleContextHolder;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhangyf on 2018/3/9.
 */
public class ResultEntityBuilder {
    private static Logger logger = LoggerFactory.getLogger(ResultEntityBuilder.class);
    private MessageSource messageSource;
    private Map<String, Object> attributes;
    private ResultEntity entity = new ResultEntity();

    public ResultEntityBuilder(MessageSource messageSource) {
        this.messageSource = messageSource;
        attributes = new HashMap<>();
    }

    public ResultEntityBuilder status(int statusCode) {
        entity.setStatus(statusCode);
        return this;
    }

    public ResultEntityBuilder message(String message) {
        entity.setMessage(message);
        return this;
    }


    public ResultEntityBuilder payload(Object payload) {
        try {
            entity.setPayload(payload);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
        return this;
    }

    public ResultEntityBuilder attr(String key, Object value) {
        attributes.put(key, value);
        return this;
    }

    /**
     * 从 data 对象中抽取出相应的属性值保持到attributes中,
     * 在执行build()方法时可以将attributes写入到entity的payload中
     *
     * @param data javabean 或者 Map对象
     * @param keys 需要抽取的属性名称集合
     * @return
     */
    public ResultEntityBuilder extract(Object data, String... keys) {
        for (String key : keys) {
            try {
                attr(key, BeanUtilsBean.getInstance().getPropertyUtils().getProperty(data, key));
            } catch (Exception e) {
                throw new IllegalArgumentException(e);
            }
        }
        return this;
    }

    /**
     * 构建ResultEntity对象
     *
     * @param messageReplacements 使用该参数替换status对应的消息字符串中的 {placeholder}字符串,生成完整的message
     * @return
     */
    public ResultEntity build(Object... messageReplacements) {
        if (!entity.success())
            generateMessage(messageReplacements);

        if (entity.getPayload() == null && !attributes.isEmpty()) {
            entity.setPayload(attributes);
        } else if (entity.getPayload() != null && !attributes.isEmpty()) {
            logger.warn("payload is not null, attributes will be ignored.");
        }
        return entity;
    }

    private ResultEntityBuilder generateMessage(Object... parameters) {
        return message(messageSource.getMessage(entity.getStatus() + "", parameters, LocaleContextHolder.getLocale()));
    }

    public static void main(String[] args) {
        ResultEntityBuilder builder = new ResultEntityBuilder(null);
        User u = new User();
        u.setId(1);
        u.setUpdateTime(new Date());
        u.setStatus("1");
        builder.extract(u, "id", "updateTime", "status");
        Object entity = builder.build();

        Map<String, java.lang.Object> map = new HashMap<>();
        map.put("id", 100);
        map.put("status", "11111111");
        map.put("updateTime", new Date());
        builder = new ResultEntityBuilder(null);
        builder.extract(map, "id", "updateTime", "status");
        entity = builder.build();
        System.out.println(entity);
    }
}
