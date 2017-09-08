var config = require('../../config');
var request = require('request');
var $ = require("../utils/utils");

var restUrl = config.rest.dbusRest;

module.exports = {
    send: function (topic, message, cb) {
        var url = $.url(restUrl, "/ctlMessage/send");
        request.post({url: url, json: {topic: topic, message: message}, forever: true}, $.resWrapper(cb));
    }
};
