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

package com.creditease.dbus.ws.common;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by dongwang47 on 2016/9/1.
 */
public class Result {
   public static final Result OK = new Result(0, "OK");
   private int status;
   private String message;

   @JsonIgnore
   private Map<String, Object> parameters;

   public Result (int status, String message) {
      this.status = status;
      this.message = message;
      parameters = new HashMap<>();
   }

   public void addParameter(String key, Object value) {
      parameters.put(key, value);
   }

   public <T> T getParameter(String key) {
      Object value = parameters.get(key);
      if(value == null) return null;
      return (T) value;
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
}
