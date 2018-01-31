var config = require('../../config');
var request = require('request');
var $ = require("../utils/utils");
var ssh = require('../utils/ssh-client');
var logger = require('../utils/logger');
var child_process = require('child_process');

var zk = config.zk.connect;

const jarCatalog = "dbus_jars";

module.exports = {
    startTopo: function (params, callback) {
        var user = params.user;
        var stormPath = params.stormPath.substring(params.stormPath.indexOf("/"));
        var jarPath = params.jarPath;
        var jarName = params.jarName;
        var dsName = params.dsName;
        var topologyType = params.topologyType;

        var idx1 = params.stormPath.indexOf(":");
        //ssh要连接的主机ip
        var ip = params.stormPath.substring(0, idx1);
        var idx2 = params.stormPath.indexOf("/");
        //ssh要连接的主机port
        var port = parseInt(params.stormPath.substring(idx1 + 1, idx2));
        //ssh连接到主机的指定目录
        var baseDir = params.stormPath.substring(idx2);
        console.log("baseDir: " + baseDir);

        var cmd = "cd "+ baseDir + "/" + jarCatalog +";" + " ./dbus_startTopology.sh " + stormPath + " " + topologyType + " " + zk + " " + dsName + " " + jarPath + jarName;
        console.log("cmd: " + cmd);
        var sshCmd = "ssh -p " + port + " " + user + "@" + ip + " " + "'" + cmd + "'";
        console.log("sshCmd: " + sshCmd);
        child_process.exec(sshCmd, function (err, stdout, stderr) {
            if(err) {
                logger.error("startTopo err: " + err);
                callback(err, stderr);
            } else if(stderr) {
                var result = [];
                result.push(stdout);
                logger.info("startTopo stderr: " + stderr);
                callback(null, result);
            } else {
                var result = [];
                result.push(stdout);
                logger.info("startTopo stdout: " + stdout);
                callback(null, result);
            }

        });
    }
};
