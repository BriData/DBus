var React = require('react');
var ReactDOM = require('react-dom');
var B = require('react-bootstrap');
var antd = require('antd');
var utils = require('../common/utils');


var TopologyUtils = {
    getSelectList: function (jarList, version, type, time) {
        return jarList.filter(function (jar) {
            return (version == null || version == jar.version)
                && (type == null || type == jar.type)
                && (time == null || time == jar.time)
        });
    },

    filterByDsType: function (jarList, dsInfo) {
        if (dsInfo == null) return jarList;
        var dsType = dsInfo.dsType;
        if (dsType == utils.dsType.mysql) {
            return jarList.filter(function (jar) {
                return jar.type == 'appender'
                    || jar.type == 'dispatcher'
                    || jar.type == 'dispatcher_appender'
                    || jar.type == 'mysql_extractor'
                    || jar.type == 'splitter_puller'
                    || jar.type == 'splitter'
                    || jar.type == 'puller';
            });
        }
        else if (dsType == utils.dsType.oracle) {
            return jarList.filter(function (jar) {
                return jar.type == 'appender'
                    || jar.type == 'dispatcher'
                    || jar.type == 'dispatcher_appender'
                    || jar.type == 'splitter_puller'
                    || jar.type == 'splitter'
                    || jar.type == 'puller';
            });
        }
        else if (dsType.startsWith("log_")) {
            return jarList.filter(function (jar) {
                return jar.type == 'log_processor';
            });
        }
        return [];
    },

    getAntOptionList: function (jarList, neededKey) {
        var keys = {};
        jarList.forEach(function (jar) {
            keys[jar[neededKey]] = true;
        });
        var ret = [];
        for (var key in keys) {
            ret.push(<antd.Select.Option value={key}>{key}</antd.Select.Option>);
        }
        return ret;
    },

    getMainClass: function (jar) {
        var verionTypeToMainClass = {
            "0.2.x/appender": "com.creditease.dbus.AppenderTopology",
            "0.2.x/dispatcher": "com.creditease.dbus.dispatcher.DispatcherTopology",
            "0.2.x/splitter": "com.creditease.dbus.DataShardsSplittingTopology",
            "0.2.x/puller": "com.creditease.dbus.DataPullTopology",

            "0.3.x/appender": "com.creditease.dbus.stream.DispatcherAppenderTopology",
            "0.3.x/dispatcher": "com.creditease.dbus.stream.DispatcherAppenderTopology",
            "0.3.x/dispatcher_appender": "com.creditease.dbus.stream.DispatcherAppenderTopology",

            "0.3.x/mysql_extractor": "com.creditease.dbus.extractor.MysqlExtractorTopology",

            "0.3.x/log_processor": "com.creditease.dbus.log.processor.DBusLogProcessorTopology",

            "0.3.x/splitter": "com.creditease.dbus.FullPullerTopology",
            "0.3.x/puller": "com.creditease.dbus.FullPullerTopology",
            "0.3.x/splitter_puller": "com.creditease.dbus.FullPullerTopology",

            "0.4.x/appender": "com.creditease.dbus.stream.DispatcherAppenderTopology",
            "0.4.x/dispatcher": "com.creditease.dbus.stream.DispatcherAppenderTopology",
            "0.4.x/dispatcher_appender": "com.creditease.dbus.stream.DispatcherAppenderTopology",

            "0.4.x/mysql_extractor": "com.creditease.dbus.extractor.MysqlExtractorTopology",

            "0.4.x/log_processor": "com.creditease.dbus.log.processor.DBusLogProcessorTopology",

            "0.4.x/splitter": "com.creditease.dbus.FullPullerTopology",
            "0.4.x/puller": "com.creditease.dbus.FullPullerTopology",
            "0.4.x/splitter_puller": "com.creditease.dbus.FullPullerTopology"
        };
        return verionTypeToMainClass[jar.version + "/" + jar.type];
    },

    getTid: function (dsName, jar) {
        var suffix = {
            "0.2.x/appender": "-appender",
            "0.2.x/dispatcher": "-dispatcher",
            "0.2.x/splitter": "-fullsplitter",
            "0.2.x/puller": "-fullpuller"
        };
        var value = suffix[jar.version + "/" + jar.type];
        if (value == null) value = "";
        return " -tid " + dsName + value;
    },

    getType: function (jar) {
        var suffix = {
            "0.3.x/appender": "appender",
            "0.3.x/dispatcher": "dispatcher",
            "0.3.x/splitter": "splitter",
            "0.3.x/puller": "puller",

            "0.4.x/appender": "appender",
            "0.4.x/dispatcher": "dispatcher",
            "0.4.x/splitter": "splitter",
            "0.4.x/puller": "puller"
        };
        var value = suffix[jar.version + "/" + jar.type];
        if (value == null) return "";
        return " -type " + value;
    }
};
module.exports = TopologyUtils;
