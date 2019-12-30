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


package com.creditease.dbus.plugins;

import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.encoders.Encoder;
import com.creditease.dbus.encoders.EncoderConf;
import com.creditease.dbus.encoders.ExtEncodeStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 手机号码脱敏：
 * 支持的格式： 13812345678/8613812345678/86-13812345678/+8613812345678/+86-13812345678
 * 不保留尾号：
 * {regex:"^(\\+?\\d{2}-?)?(\\d{3})\\d{4}(\\d{4})$",replacement:"$1$2********",encode:0}
 * 号段脱敏：
 * {regex:"^(\\+?\\d{2}-?)?(\\d{3})\\d{4}(\\d{4})$",replacement:"$1$2****$3",encode:0}
 * <p>
 * 身份证脱敏：
 * 身份证格式：
 * 类型      地区           生日            序号      校验码
 * 十八位    xxxxxx          yyyy MM dd     375         0
 * 十五位    xxxxxx          yy MM dd       75          0
 * <p>
 * 脱敏配置：
 * {regex:"^(\\d{6})(\\d{8})(\\d{3})([0-9Xx])|^(\\d{6})(\\d{6})(\\d{2})([0-9Xx])$", replacement:"$1$5********$3$4$7$8", encode:0}
 * <p>
 * 姓名脱敏:
 * 支持的格式：" 李   先   生 " 任意位置可以包含空格
 * 过敏配置：
 * {regex:"^\\s*([\\u4e00-\\u9fa5]{1}).*", replacement:"$1**", encode:0}
 */
@Encoder(type = "regex")
public class RegexStrategy implements ExtEncodeStrategy {
    private Logger logger = LoggerFactory.getLogger(getClass());
    private ExtEncodeStrategy notMatchesEncoder = new DefaultValueStrategy();
    /**
     * 参数模式
     **/
    public static final String PATTERN = "{\"regex\":\"%s\", \"replacement\":\"%s\"}";

    @Override
    public Object encode(EncoderConf conf, Object value) {
        if (value == null || value.toString().length() == 0) return value;

        RegexEncoderConfig config = JSONObject.parseObject(conf.getEncodeParam(), RegexEncoderConfig.class);
        String valStr = value.toString();
        Pattern pattern = Pattern.compile(config.getRegex());
        Matcher matcher = pattern.matcher(valStr);
        if (matcher.matches()) {
            return matcher.replaceAll(config.getReplacement());
        } else {
            logger.warn("{} can not matches {}", config.getRegex(), valStr);
            return notMatchesEncoder.encode(conf, value);
        }
    }

    public static class RegexEncoderConfig {
        private String regex;
        private String replacement;

        public RegexEncoderConfig() {
        }

        public RegexEncoderConfig(String regex, String replacement) {
            this.regex = regex;
            this.replacement = replacement;
        }

        public String getRegex() {
            return regex;
        }

        public void setRegex(String regex) {
            this.regex = regex;
        }

        public String getReplacement() {
            return replacement;
        }

        public void setReplacement(String replacement) {
            this.replacement = replacement;
        }
    }

    public interface ParamBuilder {
        default String buildParameter(RegexEncoderConfig config) {
            return String.format(PATTERN, config.getRegex(), config.getReplacement());
        }
    }
}
