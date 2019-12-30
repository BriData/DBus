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
import com.creditease.dbus.base.com.creditease.dbus.utils.RequestSender;
import com.creditease.dbus.constant.ServiceNames;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

/**
 * User: 王少楠
 * Date: 2018-06-29
 * Desc:
 */
@Service
public class ProjectUserService {
    @Autowired
    private RequestSender sender;

    public ResultEntity getUsersByProId(Integer projectId) {
        ResponseEntity<ResultEntity> result = result = sender.get(ServiceNames.KEEPER_SERVICE, "/projectUser/select-by-project-id/{id}", projectId);
        return result.getBody();
    }

    public ResponseEntity<ResultEntity> getUsersByProIdToResponse(Integer projectId) {
        return sender.get(ServiceNames.KEEPER_SERVICE, "/projectUser/select-by-project-id/{id}", projectId);
    }
}
