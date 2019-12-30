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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.text.SimpleDateFormat;


/**
 * Created by zhangyf on 2018/3/9.
 */
public class ResultEntity {
    private static ObjectMapper mapper = new ObjectMapper();

    public static final int SUCCESS = 0;
    public static final String OK = "ok";

    public static final int FAILED_CODE = -1;
    public static final String FAILED = "Failed";

    private int status;
    private String message;

    static {
        mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Object payload;

//    public static ResultEntity parse(String json, TypeReference<?> tf) {
//        try {
//            return mapper.readValue(json, tf);
//        } catch (IOException e) {
//            throw new IllegalArgumentException(e);
//        }
//    }

    public ResultEntity() {
        this.status = SUCCESS;
        this.message = OK;
    }

    public ResultEntity(int statusCode, String message) {
        this.status = statusCode;
        this.message = message;
    }

    public boolean success() {
        return status == 0;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public <T> T getPayload(Class<T> clazz) {
        return (T) payload(clazz);
    }

    public <T> T getPayload(TypeReference<T> tr) {
        return (T) payload(tr);
    }

    private Object payload(Object type) {
        try {
            if (getPayload() != null) {
                String json = mapper.writeValueAsString(payload);
                if (type instanceof Class) return mapper.readValue(json, (Class) type);
                else return mapper.readValue(json, (TypeReference) type);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public Object getPayload() {
        return payload;
    }

    public void setPayload(Object payload) {
        this.payload = payload;
    }

    public String toJSON() {
        try {
            return mapper.writeValueAsString(this);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

//    public String payloadJson() {
//        try {
//            return mapper.writeValueAsString(getPayload());
//        } catch (JsonProcessingException e) {
//            throw new IllegalArgumentException(e);
//        }
//    }

    public static void main(String[] args) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ResultEntity entity = new ResultEntity(200, "11111");
        entity.setPayload(new ResultEntity(100, "xxx"));

        String json = mapper.writeValueAsString(entity);

        ResultEntity e = mapper.readValue(json, ResultEntity.class);

        System.out.println(e.getPayload(ResultEntity.class).getMessage());
    }
}
