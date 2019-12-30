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


package com.creditease.dbus.commons.log.processor.rule.impl;

import com.creditease.dbus.commons.log.processor.rule.IRule;

public enum Rules {

    FILTER("filter") {
        @Override
        public IRule getRule() {
            return new FilterRule();
        }
    },
    KEYFILTER("keyFilter") {
        @Override
        public IRule getRule() {
            return new KeyFilterRule();
        }
    },
    TRIM("trim") {
        @Override
        public IRule getRule() {
            return new TrimRule();
        }
    },
    REPLACE("replace") {
        @Override
        public IRule getRule() {
            return new ReplaceRule();
        }
    },
    SPLIT("split") {
        @Override
        public IRule getRule() {
            return new SplitRule();
        }
    },
    SUBSTRING("subString") {
        @Override
        public IRule getRule() {
            return new SubStringRule();
        }
    },
    CONCAT("concat") {
        @Override
        public IRule getRule() {
            return new ConcatRule();
        }
    },
    SELECT("select") {
        @Override
        public IRule getRule() {
            return new SelectRule();
        }
    },
    SAVEAS("saveAs") {
        @Override
        public IRule getRule() {
            return new SaveAsRule();
        }
    },
    PREFIXORAPPEND("prefixOrAppend") {
        @Override
        public IRule getRule() {
            return new PrefixOrAppendRule();
        }
    },
    REGEXEXTRACT("regexExtract") {
        @Override
        public IRule getRule() {
            return new RegexExtractRule();
        }
    },
    TOINDEX("toIndex") {
        @Override
        public IRule getRule() {
            return new ToIndexRule();
        }
    },
    FLATTENUMS("flattenUms") {
        @Override
        public IRule getRule() {
            return new FlattenUmsRule();
        }
    },
    JSONPATH("jsonPath") {
        @Override
        public IRule getRule() {
            return new JsonPathRule();
        }
    },
    EXCLUDEFIELDRULE("excludeField") {
        @Override
        public IRule getRule() {
            return new ExcludeFieldRule();
        }
    },
    FLATTEN_JSON_ARRAY("flattenJsonArray") {
        @Override
        public IRule getRule() {
            return new FlattenJsonArrayRule();
        }
    };


    public String name;

    private Rules(String name) {
        this.name = name;
    }

    public abstract IRule getRule();

    public static Rules fromStr(String strValue) {
        switch (strValue) {
            case "filter":
                return Rules.FILTER;
            case "keyFilter":
                return Rules.KEYFILTER;
            case "trim":
                return Rules.TRIM;
            case "replace":
                return Rules.REPLACE;
            case "split":
                return Rules.SPLIT;
            case "subString":
                return Rules.SUBSTRING;
            case "concat":
                return Rules.CONCAT;
            case "select":
                return Rules.SELECT;
            case "saveAs":
                return Rules.SAVEAS;
            case "prefixOrAppend":
                return Rules.PREFIXORAPPEND;
            case "regexExtract":
                return Rules.REGEXEXTRACT;
            case "toIndex":
                return Rules.TOINDEX;
            case "flattenUms":
                return Rules.FLATTENUMS;
            case "jsonPath":
                return Rules.JSONPATH;
            case "excludeField":
                return Rules.EXCLUDEFIELDRULE;
            case "flattenJsonArray":
                return Rules.FLATTEN_JSON_ARRAY;
            default:
                throw new RuntimeException("Invalid str value: " + strValue + "for conversion to Rules");
        }
    }
}
