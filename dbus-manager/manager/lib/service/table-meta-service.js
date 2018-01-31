var config = require('../../config');
var request = require('request');
var $ = require("../utils/utils");

var restUrl = config.rest.dbusRest;
module.exports = {
    listPlainlogTable: function(param, cb) {
        var url = $.url(restUrl, "/tables/listPlainlogTable");
        request.get({url: url, json: param, forever: true}, $.resWrapper(cb));
    },
    createPlainlogTable: function(param, cb) {
        var url = $.url(restUrl, "/tables/createPlainlogTable");
        request.get({url: url, json: param, forever: true}, $.resWrapper(cb));
    },
    createPlainlogSchemaAndTable: function(param, cb) {
        var url = $.url(restUrl, "/tables/createPlainlogSchemaAndTable");
        request.get({url: url, json: param, forever: true}, $.resWrapper(cb));
    },
    search: function (param) {
        var url = $.url(restUrl, "/tables/tablename");
        return $.promiseHttp.get(url, param);
    },
    listTableField:function (param) {
        var url = $.url(restUrl, "/tables/tablefield");
        return $.promiseHttp.post(url, param);
    }
};

