var config = require('../../config');
var logger = require('../../lib/utils/logger');
var request = require('request');
var $ = require("../utils/utils");
var ssh = require('../utils/ssh-client');
var logger = require('../utils/logger');
var child_process = require('child_process');

var zk = config.zk.connect;
module.exports = {
    stormcheck: function (params, callback) {
        var path = params;
        var idx1 = path.indexOf(":");
        var user_index1 = path.indexOf("/");
        var user_index2 = path.indexOf("/",user_index1+1);
        var user = path.substring(user_index1+1, user_index2);
        //ssh要连接的主机ip
        var ip = path.substring(0, idx1);
        var idx2 = path.indexOf("/");
        //ssh要连接的主机port
        var port = parseInt(path.substring(idx1 + 1, idx2));
        //ssh连接到主机的指定目录
        var baseDir = path.substring(idx2);
        console.log("baseDir: " + baseDir);

        var cmd = "cd "+ baseDir + ";" +"pwd";
        console.log("cmd: " + cmd);
        var sshCmd = "ssh -p " + port + " " + user + "@" + ip + " " + "'" + cmd + "'";
        console.log("sshCmd: " + sshCmd);
        child_process.exec(sshCmd, function (err, stdout, stderr) {
            if(err) {
                logger.error("err: " + err);
                callback(err, stderr);
            } else if(stderr) {
                var result = [];
                result.push(stdout);
                logger.error("stderr: " + stderr);
                callback(null, result);
            } else {
                var result = [];
                result.push(stdout);
                callback(null, result);
            }

        });
    }
};
