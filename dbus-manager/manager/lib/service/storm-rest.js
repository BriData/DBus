/**
 * Created by zhangyf on 16/10/27.
 */
var conf = require('../../config');
var $ = require('../utils/utils');
var logger = require('../utils/logger');
var PropertiesReader = require('properties-reader');

var config = require('../../config');
var ZooKeeper = require('node-zookeeper-client');
var client = ZooKeeper.createClient(config.zk.connect,{retries:3});


module.exports = {
    /**
     * 使用storm rest api获取topology的状态
     * @returns {*}
     */
    listTopologies: function (param) {
        var url = $.url(param.stormRest, "/topology/summary");
        return $.promiseHttp.get(url);
    },
    stopTopologies: function (param) {
        var url = $.url(param.stormRest, "/topology/" + param.id + "/kill/" + param.waitTime);
        return $.promiseHttp.post(url);
    }
};