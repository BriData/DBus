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
    }
};
