var config = require('../../config');
var request = require('request');
var $ = require("../utils/utils");

var restUrl = config.rest.dbusRest;
module.exports = {
    insertSchema: function (param) {
        var url = $.url(restUrl, "/dataschemas");
        return $.promiseHttp.post(url, param);
    },
    insertTable:function (param) {
        var url = $.url(restUrl, "/tables");
        return $.promiseHttp.post(url, param);
    },
    insertDbusTables: function (param,cb) {
        var url = $.url(restUrl, "/dbustable/tableinsert");
        request.post({url: url, qs: param, forever: true}, $.resWrapper(cb));
    },
    listSourceTables: function (param,cb) {
        var url = $.url(restUrl, "/dbustable/listtable");
        request.get({url: url, qs: param, forever: true}, $.resWrapper(cb));
    },
    updateSchema: function (param) {
        var url = $.url(restUrl, "/dataschemas/update");
        return $.promiseHttp.post(url, param);
    }
};

