var config = require('../../config');
var request = require('request');
var $ = require("../utils/utils");

var restUrl = config.rest.dbusRest;
module.exports = {
    search: function (param, cb) {
        var url = $.url(restUrl, "/avroSchemas/search");
        request.post({url: url, json: param, forever: true}, $.resWrapper(cb));
    }
};
