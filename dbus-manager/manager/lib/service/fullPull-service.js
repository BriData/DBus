var config = require('../../config');
var request = require('request');
var $ = require("../utils/utils");

var restUrl = config.rest.dbusRest;

module.exports = {
    send: function (topic,outputTopic,message, cb) {
        var url = $.url(restUrl, "/fullPull/send");
        request.post({url: url, json: {topic: topic,outputTopic:outputTopic ,message: message}, forever: true}, $.resWrapper(cb));
    }
};
