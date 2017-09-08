var util = require('util');
var config = require('../../config');
var console = require('console');
var request = require('request');
var $ = require("../utils/utils");

var restUrl = config.rest.dbusRest;
module.exports = {
    savezk: function (param, cb) {
        var url = $.url(restUrl, "/conf/savezk");
        request.post({url: url, json: param, forever: true}, $.resWrapper(cb));
    },
}