/**
 * Created by zhangyf on 16/10/27.
 */
var conf = require('../../config');
var $ = require('../utils/utils');
var logger = require('../utils/logger');

var stormApi = conf.rest.stormRest;

module.exports = {
    /**
     * 使用storm rest api获取topology的状态
     * @returns {*}
     */
    topologies: function () {
        var url = $.url(stormApi, "/topology/summary");
        return $.promiseHttp.get(url);
    },
    workers : function(topologyId) {
        var url = $.url(stormApi, "topology-workers/"+topologyId);
        return $.promiseHttp.get(url, null, topologyId);
    }
};