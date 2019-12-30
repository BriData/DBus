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

import com.creditease.dbus.commons.log.processor.rule.impl.Rules;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class ParseRuleGrammar {

    private static Pattern rangePattern = Pattern.compile("-?[0-9]+:-?[0-9]*");

    private static Pattern enumPattern = Pattern.compile("-?([0-9]+)(,-?[0-9]+)+");

    private static Pattern singlePattern = Pattern.compile("-?[0-9]+");

    public static List<ParseResult> parse(List<RuleGrammar> grammar, int dataColumnSize, Rules ruleType) {
        List<ParseResult> rst = new ArrayList<>();
        for (RuleGrammar rg : grammar) {
            ParseResult pr = new ParseResult();

            pr.setFilterKey(rg.getFilterKey());

            if (!StringUtils.equals(ruleType.name, Rules.FLATTENUMS.name) && !StringUtils.equals(ruleType.name, Rules.KEYFILTER.name)) {
                pr.setScope(parseScope(rg.getRuleScope(), dataColumnSize));
            }

            if (!StringUtils.equals(ruleType.name, Rules.JSONPATH.name)) {
                pr.setOperate(rg.getRuleOperate());
            }

            //处理filter
            if (StringUtils.equals(ruleType.name, Rules.FILTER.name) || StringUtils.equals(ruleType.name, Rules.KEYFILTER.name)) {
                pr.setEq(parseFilterOperate(rg.getRuleOperate()));
            }

            //处理substring
            if (StringUtils.equals(ruleType.name, Rules.SUBSTRING.name)) {
                pr.setStart(rg.getSubStart());
                pr.setStartType(rg.getSubStartType());
                pr.setEnd(rg.getSubEnd());
                pr.setEndType(rg.getSubEndType());
            } else {
                pr.setParamter(rg.getRuleParamter());
            }

            if (!StringUtils.isEmpty(rg.getRuleType()) && !StringUtils.equals(ruleType.name, Rules.SUBSTRING.name)) {
                pr.setRuleType(rg.getRuleType());
            }
            rst.add(pr);
        }

        return rst;
    }

    private static boolean parseFilterOperate(String ruleOperate) {
        if (StringUtils.equals("==", ruleOperate)) {
            return true;
        } else {
            return false;
        }
    }


    private static List<Integer> parseScope(String param, int dataColumnSize) {
        List<Integer> ret;
        if (StringUtils.equals("0:", param) || StringUtils.equals("*", param)) {
            ret = all(dataColumnSize);
        } else if (singlePattern.matcher(param).matches()) {
            ret = single(param, dataColumnSize);
        } else if (rangePattern.matcher(param).matches()) {
            ret = range(param, dataColumnSize);
        } else if (enumPattern.matcher(param).matches()) {
            ret = emum(param, dataColumnSize);
        } else {
            throw new IllegalArgumentException("illegal rule scope args: " + param);
        }
        return ret;
    }

    private static List<Integer> all(int dataColumnSize) {
        return seeds(0, dataColumnSize);
    }

    private static List<Integer> single(String param, int dataColumnSize) {
        int num = NumberUtils.toInt(param);
        if (param.contains("-")) {
            num = NumberUtils.toInt(param) + dataColumnSize;
        }
        return seeds(num, num);
    }

    private static List<Integer> emum(String param, int dataColumnSize) {
        String[] arrs = StringUtils.split(param, ",");
        List<Integer> list = new ArrayList<>();
        for (String item : arrs) {
            if (item.contains("-")) {
                list.add(dataColumnSize + NumberUtils.toInt(item));
            } else {
                list.add(NumberUtils.toInt(item));
            }
        }
        return list;
    }

    private static List<Integer> range(String param, int dataColumnSize) {
        String[] arrs = StringUtils.split(param, ":");
        int start = 0, end = 0;
        if (arrs.length == 0) {
            return new ArrayList<>();
        } else if (arrs.length == 1) {
            start = NumberUtils.toInt(arrs[0]);
            if (arrs[0].contains("-")) {
                start = NumberUtils.toInt(arrs[0]) + dataColumnSize;
            }
            end = dataColumnSize;
        } else if (arrs.length == 2) {
            start = NumberUtils.toInt(arrs[0]);
            end = NumberUtils.toInt(arrs[1]);
            if (start == end) {
                return new ArrayList<>();
            }
            if (arrs[0].contains("-")) {
                start = NumberUtils.toInt(arrs[0]) + dataColumnSize;
            }
            if (arrs[1].contains("-")) {
                end = NumberUtils.toInt(arrs[1]) + dataColumnSize;
            }
        }

        if (start > end) {
            return new ArrayList<>();
        }
        return seeds(start, end);
    }

    private static List<Integer> seeds(int start, int end) {
        List<Integer> ret = new ArrayList<>();
        if (start == end) {
            ret.add(start);
        }
        for (int i = start; i < end; i++) {
            ret.add(i);
        }
        return ret;
    }

    public static void main(String[] args) {
        //System.out.println("-:" + rangePattern.matcher("-").matches());
        System.out.println("0::" + rangePattern.matcher("0:").matches());
        System.out.println("-5:-1:" + rangePattern.matcher("-5:-1").matches());
        System.out.println("0:5:" + rangePattern.matcher("0:5").matches());
        System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
        System.out.println("12,17,23,28:" + enumPattern.matcher("12,17,23,28").matches());
        System.out.println("-1,-3,-5,-7:" + enumPattern.matcher("-1,-3,-5,-7").matches());
        System.out.println("-1,-3,-5:" + enumPattern.matcher("-1,-3,-5").matches());
        System.out.println("1,2:" + enumPattern.matcher("1,2").matches());
        System.out.println("12,3,7,9:" + enumPattern.matcher("12,3,7,9").matches());
        System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
        System.out.println("0:" + singlePattern.matcher("0").matches());
        System.out.println("1:" + singlePattern.matcher("1").matches());
        System.out.println("-1:" + singlePattern.matcher("-1").matches());

        System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
        System.out.println("60:-60:" + rangePattern.matcher("60:-60").matches());

        System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
        System.out.println("0::" + parseScope("0:", 100));
        System.out.println("*:" + parseScope("*", 100));
        System.out.println("-5:-1:" + parseScope("-5:-1", 100));
        System.out.println("0:5:" + parseScope("0:5", 100));
        System.out.println("0:0:" + parseScope("0:0", 100));
        System.out.println("1:1:" + parseScope("1:1", 100));
        System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
        System.out.println("12,17,23,28:" + parseScope("12,17,23,28", 100));
        System.out.println("-1,-3,-5,-7:" + parseScope("-1,-3,-5,-7", 100));
        System.out.println("-1,-3,-5:" + parseScope("-1,-3,-5", 100));
        System.out.println("1,2:" + parseScope("1,2", 100));
        System.out.println("12,3,7,9:" + parseScope("12,3,7,9", 100));
        System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
        System.out.println("0:" + parseScope("0", 100));
        System.out.println("1:" + parseScope("1", 100));
        System.out.println("-1:" + parseScope("-1", 100));
        System.out.println("-99:" + parseScope("-99", 100));
        System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
        System.out.println("60:-60:" + parseScope("60:-60", 100));
        System.out.println("50:-50:" + parseScope("50:-50", 100));


    }
}
