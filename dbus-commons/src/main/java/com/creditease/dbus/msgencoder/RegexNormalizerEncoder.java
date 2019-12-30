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


package com.creditease.dbus.msgencoder;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.commons.DataType;
import com.creditease.dbus.commons.DbusMessage;
import com.google.common.hash.Hashing;

import java.nio.charset.Charset;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 手机号码脱敏：
 * 支持的格式： 13812345678/8613812345678/86-13812345678/+8613812345678/+86-13812345678
 * 不保留尾号：
 * {regex:"^(\\+?\\d{2}-?)?(\\d{3})\\d{4}(\\d{4})$",replaceStr:"$1$2********",encode:0}
 * 号段脱敏：
 * {regex:"^(\\+?\\d{2}-?)?(\\d{3})\\d{4}(\\d{4})$",replaceStr:"$1$2****$3",encode:0}
 * <p>
 * 身份证脱敏：
 * 身份证格式：
 * 类型      地区           生日            序号      校验码
 * 十八位    xxxxxx          yyyy MM dd     375         0
 * 十五位    xxxxxx          yy MM dd       75          0
 * <p>
 * 脱敏配置：
 * {regex:"^(\\d{6})(\\d{8})(\\d{3})([0-9Xx])|(\\d{6})(\\d{6})(\\d{2})([0-9Xx])$", replaceStr:"$1$5********$3$4$7$8", encode:0}
 * <p>
 * 姓名脱敏:
 * 支持的格式：" 李   先   生 " 任意位置可以包含空格
 * 过敏配置：
 * {regex:"^\\s*([\\u4e00-\\u9fa5]{1}).*", replaceStr:"$1**", encode:0}
 */
@Deprecated
public class RegexNormalizerEncoder implements EncodeStrategy {
    @Override
    public Object encode(DbusMessage.Field field, Object value, EncodeColumn col) {
        if (field.dataType() == DataType.STRING) {
            /*
            脱敏需要预处理时，输入框输入的是json串，例如{regex:"^[\\u4e00-\\u9fa5]{1,4}$",replaceStr:"某某",encode:1,saltParam:"8975608a345ce784d09b6ce1479722b4"}
            regex是预处理使用的正则表达式，replaceStr为用于替换的字符串,encode取值为1或0，若为1，则预处理后需再脱敏，为0，只是进行预处理
            saltParam为脱敏的盐值
             */
            JSONObject jsonValue = JSON.parseObject(col.getEncodeParam());
            if (jsonValue.getIntValue("encode") == 1) {
                return value == null ? value : md5RegexHandle(value, jsonValue);
            } else {
                return value == null ? value : regexHandle(value.toString(), jsonValue.getString("regex"), jsonValue.getString("replaceStr"));
            }
        }
        return value;
    }

    public String regexHandle(String target, String regex, String replaceStr) {
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(target);
        if (matcher.find()) {
            //若无replaceStr,则返回匹配的字符串，若有，则用replaceStr替换全部匹配的字符串
            if (replaceStr == null) {
                return matcher.group(0);
            } else {
                return matcher.replaceAll(replaceStr);
                //return target.replaceAll(matcher.group(0),replaceStr);
            }
        }
        return "";
    }

    public String md5RegexHandle(Object value, JSONObject jsonValue) {

        String regexHandled = regexHandle(value.toString(), jsonValue.getString("regex"), jsonValue.getString("replaceStr"));
        if (regexHandled.equals("")) {
            return "";
        }
        String salt = jsonValue.getString("saltParam") == null ? "" : jsonValue.getString("saltParam");

        return new DBusHashCode(Hashing.md5().hashString(regexHandled + salt, Charset.forName("UTF-8"))).toString();
    }


    public static void main(String[] args) {
        System.out.println(new RegexNormalizerEncoder().regexHandle("某某哈啊哈哈", "[\\u4e00-\\u9fa5]{1,4}", "呵呵"));
        System.out.println(new RegexNormalizerEncoder().regexHandle("abcdf", "[^abc]", null));
        System.out.println(new RegexNormalizerEncoder().regexHandle("a8呵呵", "\\W", "哈"));
        // 手机号码： 13812345678/8613812345678/86-13812345678/+8613812345678/+86-13812345678
        System.out.println(new RegexNormalizerEncoder().regexHandle("+8613812345678", "^(\\+?\\d{2}-?)?(\\d{3})\\d{4}(\\d{4})$", "$1$2****$3"));

//        xxxxxx yyyy MM dd 375 0     十八位
//        xxxxxx    yy MM dd   75 0     十五位
//        地区： [1-9]\d{5}
//        年的前两位： (18|19|([23]\d))            1800-2399
//        年的后两位： \d{2}
//        月份： ((0[1-9])|(10|11|12))
//        天数： (([0-2][1-9])|10|20|30|31)          闰年不能禁止29+
//        三位顺序码： \d{3}
//        两位顺序码： \d{2}
//        校验码： [0-9Xx]

        // 身份证：45032619840627183x 43312319791130094X
        System.out.println(new RegexNormalizerEncoder().regexHandle("43cc312319791130094X", "^(?:(\\d{6})(\\d{8})(\\d{3})([0-9Xx]))|(?:(\\d{6})(\\d{6})(\\d{2})([0-9Xx]))$", "$1$5********$3$4$7$8"));
        // 姓名
        System.out.println(new RegexNormalizerEncoder().regexHandle("李先生", "^\\s*([\\u4e00-\\u9fa5]{1}).*", "$1**"));
        // 座机号码 010-12345678
        System.out.println(new RegexNormalizerEncoder().regexHandle("01012345678", "^\\s*(\\d{3,4})\\s*(-)?\\s*\\d{7,8}\\s*", "$1********"));


    }
}
