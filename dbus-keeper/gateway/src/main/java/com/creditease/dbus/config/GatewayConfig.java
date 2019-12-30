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


package com.creditease.dbus.config;

import com.creditease.dbus.filter.AuthenticationFilter;
import org.springframework.cloud.client.loadbalancer.LoadBalanced;
import org.springframework.cloud.netflix.zuul.filters.discovery.PatternServiceRouteMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by zhangyf on 2018/3/8.
 */
@Configuration
public class GatewayConfig {
    @Bean
    public AuthenticationFilter accessFilter() {
        return new AuthenticationFilter();
    }

    @Bean
    public PatternServiceRouteMapper serviceRouteMapper() {
        return new PatternServiceRouteMapper("(?<name>^.+)-(?<version>v.+$)", "${version}/${name}");
    }

    @Bean
    @LoadBalanced
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }


    public static void main(String[] args) {
        String ptn = "(?<name>^.+)-(?<version>v.+$)";
        String x = "keeper-mgr-v1";
        Pattern p = Pattern.compile(ptn);
        Matcher matcher = p.matcher(x);

        if (matcher.matches()) {
            System.out.println(matcher.group("name"));
            System.out.println(matcher.group("version"));
        }
    }
}
