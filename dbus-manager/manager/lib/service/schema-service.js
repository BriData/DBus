var config = require('../../config');
var request = require('request');
var $ = require("../utils/utils");

var restUrl = config.rest.dbusRest;

module.exports = {
    list: function (dsId, cb) {
        var url = $.url(restUrl, "/dataschemas");
        request({url: url, qs: {dsId: dsId}, forever: true}, $.resWrapper(cb));
    },

    listStatus: function (schemaName, cb) {
        var url = $.url(restUrl, "/dataschemas/"+schemaName);
        request.get({url: url, forever: true}, $.resWrapper(cb));
    },

    listByDsName: function (dsName, cb) {
        var url = $.url(restUrl, "/dataschemas/schemaname");
        request.get({url: url, qs: {dsName: dsName},forever: true}, $.resWrapper(cb));
    },
    listBySchemaName: function (param, cb) {
        var url = $.url(restUrl, "/dataschemas/checkschema");
        request.get({url: url, qs: param, forever: true}, $.resWrapper(cb));
    }
};
