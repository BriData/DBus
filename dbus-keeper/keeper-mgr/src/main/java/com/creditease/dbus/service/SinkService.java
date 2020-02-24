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
import com.creditease.dbus.domain.model.Sink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * Created by xiancangao on 2018/3/28.
 */
@Service
public class SinkService {
    @Autowired
    private RequestSender sender;

    protected Logger logger = LoggerFactory.getLogger(getClass());

    public ResultEntity createSink(Sink sink) {
        ResponseEntity<ResultEntity> result = sender.post(ServiceNames.KEEPER_SERVICE, "/sinks/create", sink);
        return result.getBody();
    }

    public ResultEntity updateSink(Sink sink) {
        ResponseEntity<ResultEntity> result = sender.post(ServiceNames.KEEPER_SERVICE, "/sinks/update", sink);
        return result.getBody();
    }

    public ResultEntity deleteSink(Integer id) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/sinks/delete/{0}", id);
        return result.getBody();
    }

    public ResultEntity search(String queryString) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/sinks/search", queryString);
        return result.getBody();
    }

    public boolean sinkTest(String url) {
        String[] urls = url.split(",");
        Socket socket = null;
        try {
            for (String s : urls) {
                socket = new Socket();
                String[] ipPort = s.split(":");
                if (ipPort == null || ipPort.length != 2) {
                    return false;
                }
                socket.connect(new InetSocketAddress(ipPort[0], Integer.parseInt(ipPort[1])));
                socket.close();
            }
            logger.info("sink连通性测试通过.{}", url);
            return true;
        } catch (IOException e) {
            logger.error("sink连通性测试异常.==={};==={}", url, e.getMessage(), e);
            return false;
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
    }

    public ResultEntity searchByUserProject(String queryString) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/sinks/search-by-user-project", queryString);
        return result.getBody();
    }

}
