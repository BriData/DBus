/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2018 Bridata
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
import com.creditease.dbus.base.com.creditease.dbus.utils.RequestSender;
import com.creditease.dbus.constant.ServiceNames;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

/**
 * Created by Administrator on 2018/8/5.
 */
@Service
public class FlowLineCheckService {

    @Autowired
    private RequestSender sender;

    public ResultEntity checkFlowLine(Integer tableId) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/flow-line-check//check/{0}", "", tableId);
        return result.getBody();
    }

}
