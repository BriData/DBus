var util = require('util');
var config = require('../../config');
var console = require('console');
var request = require('request');
var $ = require("../utils/utils");

var restUrl = config.rest.dbusRest;
module.exports = {
    getPassword: function(dsId, cb) {
        var url = $.url(restUrl, "/datasources/"+dsId);
        request({url: url, forever: true}, $.resWrapper(cb));
    },
    search: function (param, cb) {
        var url = $.url(restUrl, "/tables/search");
        request.post({url: url, json: param, forever: true}, $.resWrapper(cb));
    },
    list: function (param, cb) {
        var url = $.url(restUrl, "/tables/search");
        request.get({url: url, json: param, forever: true}, $.resWrapper(cb));
    },
    updateTable: function (param, cb) {
        var url = $.url(restUrl, "/tables/update");
        request.post({url: url, json: param, forever: true}, $.resWrapper(cb));
    },
    pullWhole: function (param, cb) {
        var url = $.url(restUrl, "/tables/activate/"+param.id);
        request.post({url: url, json:param, forever: true}, $.resWrapper(cb));
    },
    pullIncrement: function (param, cb) {
        var url = $.url(restUrl, "/tables/activate/"+param.id);
        request.post({url: url, json:param, forever: true}, $.resWrapper(cb));
    },
    executeSql: function (param, cb) {
        var url = $.url(restUrl, "/tables/executeSql");
        request({url: url, json: param, forever: true}, $.resWrapper(cb));
    },
    stop: function (param,cb) {
        var url = $.url(restUrl, "/tables/deactivate/"+param.id);
        request.post({url: url,forever: true}, $.resWrapper(cb));
    },
    confirmStatusChange: function(param,cb) {
        var url = $.url(restUrl, "/tables/confirmStatusChange");
        request({url: url, json: param, forever: true}, $.resWrapper(cb));
    },
    getVersionListByTableId: function(param,cb) {
        var url = $.url(restUrl, "/tables/getVersionListByTableId");
        request({url: url, json: param, forever: true}, $.resWrapper(cb));
    },
    getVersionDetail: function(param,cb) {
        var url = $.url(restUrl, "/tables/getVersionDetail");
        request({url: url, json: param, forever: true}, $.resWrapper(cb));
    },
    desensitization: function(param,cb) {
        var url = $.url(restUrl, "/tables/desensitization");
        request({url: url, json: param, forever: true}, $.resWrapper(cb));
    },
    fetchTableColumns: function(param,cb) {
        var url = $.url(restUrl, "/tables/fetchTableColumns");
        request({url: url, json: param, forever: true}, $.resWrapper(cb));
    },
    fetchEncodeAlgorithms: function(cb) {
        var url = $.url(restUrl, "/tables/fetchEncodeAlgorithms");
        request({url: url, forever: true}, $.resWrapper(cb));
    },

    changeDesensitization: function(param,cb) {        
        var url = $.url(restUrl, "/tables/changeDesensitization");
        request({url: url, json: param, forever: true}, $.resWrapper(cb));
    },
    listTable: function (dsName, schemaName, cb) {
        var url = $.url(restUrl, "/tables/tablename");
        request.get({url: url, qs: {dsName: dsName,schemaName:schemaName},forever: true}, $.resWrapper(cb));
    },
    listTableField: function (dsName, schemaName, tableName, cb) {
        var url = $.url(restUrl, "/tables/tablefield");
        request.get({url: url, qs: {dsName: dsName,schemaName:schemaName,tableName:tableName},forever: true}, $.resWrapper(cb));
    },
    listManagerTables:function (param) {
        var url = $.url(restUrl, "/tables/schemaname");
        return $.promiseHttp.get(url, param);
    },
    listAllManagerTables: function (cb) {
        var url = $.url(restUrl, "/tables/allTables/");
        request.get({url: url, forever: true}, $.resWrapper(cb));
    },
    searchRules:function (param,cb) {
        var url = $.url(restUrl, "/tables/searchRules");
        request.get({url: url,  json: param, forever: true}, $.resWrapper(cb));
    },
    saveRules:function (param,cb) {
        var url = $.url(restUrl, "/tables/saveRules");
        request.get({url: url,  json: param, forever: true}, $.resWrapper(cb));
    },
    executeSqlRule:function (param,cb) {
        var url = $.url(restUrl, "/tables/executeSqlRule");
        request.post({url: url,  json: param, forever: true}, $.resWrapper(cb));
    },
    readKafkaTopic:function (param,cb) {
        var url = $.url(restUrl, "/tables/readKafkaTopic");
        request.get({url: url,  json: param, forever: true}, $.resWrapper(cb));
    }
};
