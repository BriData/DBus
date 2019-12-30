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
import com.creditease.dbus.domain.model.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

/**
 * Created by zhangyf on 2018/3/21.
 */
@Service
public class UserService {
    @Autowired
    private RequestSender sender;
    private static final String MS_SERVICE = ServiceNames.KEEPER_SERVICE;

    //@HystrixCommand()
    public ResultEntity createUser(User user) {
        ResponseEntity<ResultEntity> result = sender.post(MS_SERVICE, "/users/create", user);
        return result.getBody();
    }

    public ResultEntity updateUser(User user) {
        ResponseEntity<ResultEntity> result = sender.post(MS_SERVICE, "/users/update/" + user.getId(), user);
        return result.getBody();
    }

    public ResultEntity deleteUser(Integer id) {
        ResponseEntity<ResultEntity> result = sender.get(MS_SERVICE, "/users/delete/{0}", id);
        return result.getBody();
    }

    public ResultEntity getUser(int id) {
        ResponseEntity<ResultEntity> result = sender.get(MS_SERVICE, "/users/{0}", id);
        return result.getBody();
    }

    public ResultEntity search(User user, Integer pageNum, Integer pageSize, String sortby, String order) {
        String url = "/users/search?pageNum=" + pageNum + "&pageSize=" + pageSize + "&sortby=" + sortby + "&order=" + order;
        ResponseEntity<ResultEntity> result = sender.post(MS_SERVICE, url, user);
        return result.getBody();
    }

    public ResultEntity queryByEmail(String email) {
        ResponseEntity<ResultEntity> result = sender.get(MS_SERVICE, "/users/query-by-email", email);
        return result.getBody();
    }

    public ResultEntity modifyPassword(String oldPassword, String newPassword, Integer id) {
        String query = "?oldPassword={0}&newPassword={1}&id={2}";
        ResponseEntity<ResultEntity> result = sender.get(MS_SERVICE, "/users/modify-password", query, oldPassword, newPassword, id);
        return result.getBody();
    }
}
