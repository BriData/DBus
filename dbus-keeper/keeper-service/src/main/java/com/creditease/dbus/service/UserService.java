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

import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.domain.mapper.UserMapper;
import com.creditease.dbus.domain.model.User;
import com.creditease.dbus.utils.DBusUtils;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * Created by zhangyf on 2018/3/9.
 */
@Service
public class UserService {
    @Autowired
    private UserMapper mapper;

    public void createUser(User user) {
        mapper.insert(user);
    }

    public User getUser(int id) {
        return mapper.selectByPrimaryKey(id);
    }

    public PageInfo<User> search(User user, int pageNum, int pageSize, String sortby, String order) {
        Map<String, Object> map = DBusUtils.object2map(user);
        map.put("sortby", sortby);
        map.put("order", order);
        PageHelper.startPage(pageNum, pageSize);
        List<User> users = mapper.search(map);
        // 分页结果
        PageInfo page = new PageInfo(users);
        return page;
    }

    public User queryByEmail(String email) {
        return mapper.selectByEmail(email);
    }

    public void updateUser(User user) {
        mapper.updateByPrimaryKey(user);
    }

    public void deleteUser(Integer id) {
        mapper.deleteByPrimaryKey(id);
    }

    public int modifyPassword(String oldPassword, String newPassword, Integer id) {
        User user = mapper.selectByPrimaryKey(id);
        if (!user.getPassword().equals(oldPassword)) {
            return MessageCode.OLD_PASSWORD_WRONG;
        }
        mapper.modifyPassword(newPassword, id);
        return 0;
    }
}
