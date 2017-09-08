var config = require('../../config');
var request = require('request');
var $ = require("../utils/utils");

var restUrl = config.rest.dbusRest;
module.exports = {
    search: function (param) {
        var url = $.url(restUrl, "/tables/tablename");
        return $.promiseHttp.get(url, param);
    },
    listTableField:function (param) {
        var url = $.url(restUrl, "/tables/tablefield");
        return $.promiseHttp.get(url, param);
    }
};

