var config = require('../../config');
var request = require('request');
var $ = require("../utils/utils");
var ssh = require('../utils/ssh-client');
var logger = require('../utils/logger');
var child_process = require('child_process');

var zk = config.zk.connect;
module.exports = {
    startTopo: function (params, callback) {
        var user = params.user;
        var dsName = params.dsName;
        var path = params.path;
        var topologyType = params.topologyType;

        var idx1 = path.indexOf(":");
        //ssh要连接的主机ip
        var ip = path.substring(0, idx1);
        var idx2 = path.indexOf("/");
        //ssh要连接的主机port
        var port = parseInt(path.substring(idx1 + 1, idx2));
        //var idx3 = path.lastIndexOf("/");
        //ssh连接到主机的指定目录
        var baseDir = path.substring(idx2);
        console.log("baseDir: " + baseDir);

        var cmd = "cd "+ baseDir + ";" + " ./startTopology.sh " + topologyType + " " + zk + " " + dsName;
        console.log("cmd: " + cmd);
        var sshCmd = "ssh -p " + port + " " + user + "@" + ip + " " + "'" + cmd + "'";
        console.log("sshCmd: " + sshCmd);
        child_process.exec(sshCmd, function (err, stdout, stderr) {
            if(err) {
                logger.info("err: " + err);
                callback(err, stderr);
            } else if(stderr) {
                var result = [];
                result.push(stdout);
                logger.info("stderr: " + stderr);
                callback(null, result);
            } else {
                var result = [];
                result.push(stdout);
                callback(null, result);
            }

        });
    }
};
