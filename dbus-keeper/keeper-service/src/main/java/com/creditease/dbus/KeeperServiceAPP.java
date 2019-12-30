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

import com.creditease.dbus.commons.IZkService;
import com.creditease.dbus.commons.ZkService;
import com.creditease.dbus.utils.StormToplogyOpHelper;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.netflix.eureka.EnableEurekaClient;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.client.RestTemplate;

/**
 * Created by zhangyf on 2018/3/7.
 */
@SpringBootApplication
@EnableDiscoveryClient
@EnableAutoConfiguration(exclude = {MongoAutoConfiguration.class, MongoDataAutoConfiguration.class})
@MapperScan("com.creditease.dbus.domain.mapper")
@EnableEurekaClient
@EnableScheduling
public class KeeperServiceAPP {

    @Autowired
    private Environment env;

    @Bean
    RestTemplate restTemplate() {
        return new RestTemplate();
    }

    @Bean
    IZkService zkService() throws Exception {
        return new ZkService(env.getProperty("zk.str"));
    }

    @Bean
    StormToplogyOpHelper stormTopoHelper() throws Exception {
        return new StormToplogyOpHelper(new ZkService(env.getProperty("zk.str")));
    }

    public static void main(String[] args) {
        SpringApplication.run(KeeperServiceAPP.class, args);
    }
}
