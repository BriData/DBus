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
 * Created with IntelliJ IDEA
 * Description:
 * User: 王少楠
 * Date: 2018-04-18
 * Time: 下午6:28
 */
@Service
public class ProjectResourceService {

    @Autowired
    private RequestSender sender;

    public ResultEntity queryResources(String queryString) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/projectResource/search", queryString);
        return result.getBody();
    }

    public ResultEntity queryProjectResources(String queryString) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/projectResource/project-resource", queryString);
        return result.getBody();
    }

    public ResultEntity getEncodeHint(String queryString) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/projectEncodeHint/select-by-pid-tid", queryString);
        return result.getBody();
    }

    public ResultEntity getDSNames(String queryString) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/projectResource/datasourceNames", queryString);
        return result.getBody();
    }

    public ResultEntity queryProjectNames() {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/projectResource/project-names");
        return result.getBody();
    }

}
