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


package com.creditease.dbus.commons.log.processor.parse;

import java.io.Serializable;
import java.util.List;

/**
 * Created by Administrator on 2017/9/20.
 */
public class ParseResult implements Serializable {

    /**
     * 适用于所有算子表示索引
     */
    private List<Integer> scope;

    /**
     * filter算子表示包含
     */
    private Boolean isEq;

    /**
     * sub算子截取开始位置
     */
    private String start;

    /**
     * sub算子截取结束位置
     */
    private String end;

    /**
     * sub算子截取开始类型
     */
    private String startType;

    /**
     * sub算子截取结束类型
     */
    private String endType;

    /**
     * 适用于filter和sub以外算子
     */
    private String paramter;

    /**
     * replace算子替换字段
     */
    private String operate;

    /**
     * 表示规则类型是字符串、正则表达式或索引
     */
    private String ruleType;

    /**
     * 表示keyFilter算子中的key
     */
    private String filterKey;

    public List<Integer> getScope() {
        return scope;
    }

    public void setScope(List<Integer> scope) {
        this.scope = scope;
    }

    public Boolean getEq() {
        return isEq;
    }

    public void setEq(Boolean eq) {
        isEq = eq;
    }

    public void setStart(String start) {
        this.start = start;
    }

    public void setEnd(String end) {
        this.end = end;
    }

    public String getStart() {
        return start;
    }

    public String getEnd() {
        return end;
    }

    public String getStartType() {
        return startType;
    }

    public void setStartType(String startType) {
        this.startType = startType;
    }

    public String getEndType() {
        return endType;
    }

    public void setEndType(String endType) {
        this.endType = endType;
    }


    public String getParamter() {
        return paramter;
    }

    public void setParamter(String paramter) {
        this.paramter = paramter;
    }

    public String getOperate() {
        return operate;
    }

    public void setOperate(String operate) {
        this.operate = operate;
    }

    public String getRuleType() {
        return ruleType;
    }

    public void setRuleType(String ruleType) {
        this.ruleType = ruleType;
    }

    public String getFilterKey() {
        return filterKey;
    }

    public void setFilterKey(String filterKey) {
        this.filterKey = filterKey;
    }
}
