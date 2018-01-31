var config = require('../../config');
var request = require('request');
var $ = require("../utils/utils");

var restUrl = config.rest.dbusRest;

module.exports = {
    search: function (param, cb) {
        var url = $.url(restUrl, "/dataschemas/search");
        request.post({url: url, json: param, forever: true}, $.resWrapper(cb));
    },
    searchAll: function (cb) {
        var url = $.url(restUrl, "/dataschemas/searchAll");
        request.post({url: url, forever: true}, $.resWrapper(cb));
    },
    update: function (param,cb) {
        var url = $.url(restUrl, "/dataschemas/update");
        console.log("request url:" + url);
        request.post({url: url, json: param, forever: true}, $.resWrapper(cb));
    },
    deleteSchema: function (param,cb) {
        var url = $.url(restUrl, "/dataschemas/deleteSchema");
        request.get({url: url, json: param, forever: true}, $.resWrapper(cb));
    },
    listManagerSchemaByDsId:function (dsId,cb) {
        var url = $.url(restUrl, "/dataschemas/dsId");
        var id = parseInt(dsId);
        request.get({url: url,qs: {"dsId":id},forever: true}, $.resWrapper(cb));
    }
    
};
