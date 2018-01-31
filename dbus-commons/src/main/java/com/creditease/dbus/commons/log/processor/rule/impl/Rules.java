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
    EXCLUDEFIELDRULE("excludeField") {
        @Override
        public IRule getRule() {
            return new ExcludeFieldRule();
        }
    };;


    public String name;

    private Rules(String name) {
        this.name = name;
    }

    public abstract IRule getRule();

    public static Rules fromStr(String strValue) {
        switch(strValue) {
            case    "filter": return Rules.FILTER;
            case    "keyFilter": return Rules.KEYFILTER;
            case    "trim": return Rules.TRIM;
            case    "replace": return Rules.REPLACE;
            case    "split": return Rules.SPLIT;
            case    "subString": return Rules.SUBSTRING;
            case    "concat": return Rules.CONCAT;
            case    "select": return Rules.SELECT;
            case    "saveAs": return Rules.SAVEAS;
            case    "prefixOrAppend": return Rules.PREFIXORAPPEND;
            case    "regexExtract": return Rules.REGEXEXTRACT;
            case    "toIndex": return Rules.TOINDEX;
            case    "flattenUms": return Rules.FLATTENUMS;
            case    "excludeField": return Rules.EXCLUDEFIELDRULE;
            default:
                throw new RuntimeException("Invalid str value: " + strValue + "for conversion to Rules");
        }
    }
}
