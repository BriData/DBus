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


package com.creditease.dbus;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import javax.websocket.server.ServerEndpointConfig;

/**
 * Created by mal on 2018/4/26.
 */
public class EndpointConfigure extends ServerEndpointConfig.Configurator implements ApplicationContextAware {

    private static ApplicationContext ac;

    @Override
    public void setApplicationContext(ApplicationContext ac) throws BeansException {
        EndpointConfigure.ac = ac;
    }

    @Override
    public <T> T getEndpointInstance(Class<T> clazz) throws InstantiationException {
        T endPoint = EndpointConfigure.ac.getAutowireCapableBeanFactory().getBean(clazz);
        return (endPoint != null) ? endPoint : EndpointConfigure.ac.getAutowireCapableBeanFactory().createBean(clazz);
    }
}
