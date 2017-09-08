var config = require('../../config');
var request = require('request');
var $ = require("../utils/utils");
var ssh = require('../utils/ssh-client');

var restUrl = config.rest.dbusRest;
var baseDir = config.sshConfig.baseDir
module.exports = {
    search: function (param) {
        var url = $.url(restUrl, "/topologies");
        return $.promiseHttp.get(url, param);
    },
    add: function (param,cb) {
        var url = $.url(restUrl, "/topologies");
        request.post({url: url, json: param, forever: true}, $.resWrapper(cb));
    },
    listJars: function (callback) {
        ssh.exec("ls -R " + baseDir, null, function (err, stdout) {
            if(err) {
                callback(err);
            }
            var result = [];
            var dirs = stdout.split("\n\n");
            dirs.forEach(function (dir) {
                var arr = dir.split("\n");
                if (arr && arr.length > 1) {
                    var parent = arr[0].substr(1, arr[0].length - 2);
                    for (var i = 1; i < arr.length; i++) {
                        if (arr[i].endWith(".jar"))
                            result.push(("/"+parent).replace(baseDir, "") + "/" + arr[i]);
                    }
                }
            });
            console.log(result);
            callback(null, result);
        });
    }
};
