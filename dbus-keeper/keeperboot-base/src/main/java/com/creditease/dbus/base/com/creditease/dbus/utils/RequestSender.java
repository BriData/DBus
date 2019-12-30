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


package com.creditease.dbus.base.com.creditease.dbus.utils;

import com.creditease.dbus.base.ResultEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

/**
 * Created by zhangyf on 2018/3/21.
 */
@Component
public class RequestSender {

    @Autowired(required = false)
    private RestTemplate t;

    public ResponseEntity<ResultEntity> post(String service, String path, Object data) {
        URLBuilder ub = new URLBuilder(service, path);
        return post(ub, data);
    }

    public ResponseEntity<ResultEntity> post(URLBuilder ub, Object data) {
        return post(ub.build(), data);
    }

    public ResponseEntity<ResultEntity> post(String url, Object data) {
        HttpEntity<Object> postEntity = PostEntityFactory.create(data);
        return t.postForEntity(url, postEntity, ResultEntity.class);
    }

    public ResponseEntity<ResultEntity> get(String service, String path, Object... params) {
        return get(service, path, null, params);
    }

    public ResponseEntity<ResultEntity> get(String service, String path, String query, Object... params) {
        URLBuilder ub = new URLBuilder(service, path);
        return get(ub.build(query), params);
    }

    public ResponseEntity<ResultEntity> get(URLBuilder ub, String query, Object... params) {
        return get(ub.build(query), params);
    }

    public ResponseEntity<ResultEntity> get(String url, Object... params) {
        return t.getForEntity(url, ResultEntity.class, params);
    }
}
