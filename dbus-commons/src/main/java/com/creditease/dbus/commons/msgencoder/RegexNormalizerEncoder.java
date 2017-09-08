/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2017 Bridata
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

package com.creditease.dbus.commons.msgencoder;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.commons.DataType;
import com.creditease.dbus.commons.DbusMessage;
import com.google.common.hash.Hashing;

import java.nio.charset.Charset;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexNormalizerEncoder  implements EncodeStrategy{
    @Override
    public Object encode(DbusMessage.Field field, Object value, EncodeColumn col) {
        if (field.dataType() == DataType.STRING) {
            /*
            脱敏需要预处理时，输入框输入的是json串，例如{regex:"^[\\u4e00-\\u9fa5]{1,4}$",replaceStr:"某某",encode:1,saltParam:"8975608a345ce784d09b6ce1479722b4"}
            regex是预处理使用的正则表达式，replaceStr为用于替换的字符串,encode取值为1或0，若为1，则预处理后需再脱敏，为0，只是进行预处理
            saltParam为脱敏的盐值
             */
            JSONObject jsonValue = JSON.parseObject(col.getEncodeParam());
            if(jsonValue.getIntValue("encode") == 1){
                return value == null ? value : md5RegexHandle(value,jsonValue);
            }else{
                return value == null ? value : regexHandle(value.toString(),jsonValue.getString("regex"),jsonValue.getString
                        ("replaceStr"));
            }
        }
        return value;
    }

    public String  regexHandle(String target ,String regex,String replaceStr){
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(target);
        if (matcher.find()) {
            //若无replaceStr,则返回匹配的字符串，若有，则用replaceStr替换全部匹配的字符串
            if(replaceStr == null){
                return matcher.group(0);
            }else{
                return matcher.replaceAll(replaceStr);
                //return target.replaceAll(matcher.group(0),replaceStr);
            }
        }
        return "";
    }

    public String md5RegexHandle(Object value, JSONObject jsonValue){

        String regexHandled = regexHandle(value.toString(),jsonValue.getString("regex"),jsonValue.getString("replaceStr"));
        if(regexHandled.equals("")){
            return "";
        }
        String salt = jsonValue.getString("saltParam") == null ? "" :  jsonValue.getString("saltParam");

        return new DBusHashCode(Hashing.md5().hashString(regexHandled + salt, Charset.forName("UTF-8"))).toString();
    }


    //public static void main(String[] args) {
      //System.out.println(new RegexNormalizerEncoder().regexHandle("某某哈啊哈哈","[\\u4e00-\\u9fa5]{1,4}","呵呵"));
      //System.out.println(new RegexNormalizerEncoder().regexHandle("abcdf","[^abc]" ,null));
        // System.out.println(new RegexNormalizerEncoder().regexHandle("a8呵呵","\\W" ,"哈"));
    //}
}
