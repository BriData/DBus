var config = require('../../config');
var request = require('request');
var $ = require("../utils/utils");

var restUrl = config.rest.dbusRest;
module.exports = {
    getDatasource: function(dsId, cb) {
        var url = $.url(restUrl, "/datasources/"+dsId);
        request({url: url, forever: true}, $.resWrapper(function(err, res) {
            if(err) {
                cb(err);
            } else {
                var result = JSON.parse(res.body);
                cb(null, result);
            }
        }));
    },
    getPassword: function(dsId, cb) {
        var url = $.url(restUrl, "/datasources/"+dsId);
        request({url: url, forever: true}, $.resWrapper(cb));
    },
    list: function (cb) {
        var url = $.url(restUrl, "/datasources");
        request({url: url, forever: true}, $.resWrapper(cb));
    },
    search: function (param, cb) {
        var url = $.url(restUrl, "/datasources/search");
        request.post({url: url, json: param, forever: true}, $.resWrapper(cb));
    },
    deleteDataSource: function (param, cb) {
        var url = $.url(restUrl, "/datasources/deleteDataSource");
        request.get({url: url, json: param, forever: true}, $.resWrapper(cb));
    },
    searchFromSource: function (param, cb) {
        var url = $.url(restUrl, "/datasources/searchFromSource");
        request({url: url, json: param, forever: true}, $.resWrapper(cb));
    },
    add: function (param,cb) {
        var url = $.url(restUrl, "/datasources");
        request.post({url: url, json: param, forever: true}, $.resWrapper(cb));
    },
    validate: function (param,cb) {
        var url = $.url(restUrl, "/datasources/validate");
        request({url: url, json: param, forever: true}, $.resWrapper(cb));
    },
    active: function (param,cb) {
        var url = $.url(restUrl, "/datasources/update");
        request.post({url: url,json: param, forever: true}, $.resWrapper(cb));
    },
    inactive: function (param,cb) {
        var url = $.url(restUrl, "/datasources/update");
        request.post({url: url,json: param,forever: true}, $.resWrapper(cb));
    },
    updateDs: function (param,cb) {
        var url = $.url(restUrl, "/datasources/update");
        request.post({url: url, json: param, forever: true}, $.resWrapper(cb));
    },
    check: function (param,cb) {
        var url = $.url(restUrl, "/datasources/checkName");
        request({url: url, json: param, forever: true}, $.resWrapper(cb));
    }
};
