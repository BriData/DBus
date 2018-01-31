 global.isDebug = false;

Date.prototype.format = function(fmt) {
    var o = {
        "M+": this.getMonth() + 1, //月份
        "d+": this.getDate(), //日
        "h+": this.getHours(), //小时
        "m+": this.getMinutes(), //分
        "s+": this.getSeconds(), //秒
        "q+": Math.floor((this.getMonth() + 3) / 3), //季度
        "S": this.getMilliseconds() //毫秒
    };
    if (/(y+)/.test(fmt)) fmt = fmt.replace(RegExp.$1, (this.getFullYear() + "").substr(4 - RegExp.$1.length));
    for (var k in o) {
        if (new RegExp("(" + k + ")").test(fmt)) {
            fmt = fmt.replace(RegExp.$1, (RegExp.$1.length == 1) ? (o[k]) : (("00" + o[k]).substr(("" + o[k]).length)));
        }
    }
    return fmt;
};

module.exports = {
    loading: null, // 记载./loading.js时初始化
    basePath: "/mgr",
    builPath: function(path) {
        return [this.basePath, path].join("/");
    },
    contains : function(arr, obj) {
        for (var i = 0; i < arr.length; i++) {
            if (arr[i] === obj) {
                return true;
            }
        }
        return false;
    },
    extends: (function() {
        return Object.assign || function(target) {
                for (var i = 1; i < arguments.length; i++) {
                    var source = arguments[i];
                    for (var key in source) {
                        if (Object.prototype.hasOwnProperty.call(source, key)) {
                            target[key] = source[key];
                        }
                    }
                }
                return target;
            };
    })(),
    showLoading: function(text, icon) {
        this.loading.show(text, icon);
    },
    hideLoading: function() {
        this.loading.hide();
    },
    isEmpty: function (obj) {
        for (var name in obj) {
            if (obj.hasOwnProperty(name)) {
                return false;
            }
        }
        return true;
    },
    trim: function (str) {
        if (this.isEmpty(str)) return str;
        return str.replace(/(^\s*)|(\s*$)/g, "");
    },
    getFixedDataTablePageSize: function() {
        return parseInt(document.documentElement.clientHeight / 36) - 6;
    },
    objectDeepEqual: function (obj1, obj2, level) {
        if (obj1 == obj2) return true;
        if (obj1 == null || obj2 == null) return false;
        if (typeof obj1 != "object" || typeof obj2 != "object") return false;
        if (level <= 0) return true;
        var keyList1 = Object.keys(obj1);
        var keyList2 = Object.keys(obj2);
        if (keyList1.length != keyList2.length) return false;
        for (var key in obj1) {
            if (!this.objectDeepEqual(obj1[key], obj2[key], level - 1)) return false;
        }
        return true;
    },
    dsType: {
        mysql: 'mysql',
        oracle: 'oracle',
        logLogstash: 'log_logstash',
        logLogstashJson: 'log_logstash_json',
        logUms: 'log_ums',
        logFlume: 'log_flume',
        logFilebeat: 'log_filebeat',
        mongo: 'mongo'
    },
    logProcessor: 'log-processor'
};
