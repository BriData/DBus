var express = require('express');
var router = express.Router();
var service = require('../lib/service/topology-service');
var stormApi = require('../lib/service/storm-rest');
var logger = require('../lib/utils/logger');
var util = require('util');
var child_process = require('child_process');
var ConfUtils = require('../lib/utils/ConfUtils');


router.get('/list', function (req, res) {
    var param = req.query;
    helper.listTopologies(param).then(function (data) {
        res.json({status: 200, data: data});
    });
});

router.get('/stop', function (req, res) {
    var param = req.query;
    helper.stopTopologies(param).then(function (data) {
        res.json({status: 200, data: data});
    });
});

router.get('/start', function (req, res) {
    var param = req.query;
    helper.startTopologies(param, function (err, data) {
        if (err) {
            res.json({status: -1, data: data});
            return;
        }
        res.json({status: 200, data: data});
    });
});


var helper = {

    buildParam: function (query, keyList) {
        var param = {};
        keyList.forEach(function (key) {
            if (query[key]) {
                param[key] = query[key];
            }
        });
        return param;
    },

    stopTopologies: function(param) {
        return new Promise(function (resolve, reject) {
            stormApi.stopTopologies(param).then(function (data) {
                resolve(data);
            }).catch(function (e) {
                reject(e);
            });
        });
    },

    startTopologies: function(param, callback) {
        var user = param.user;
        var tid = param.tid;
        var stormStartScriptPath = param.stormStartScriptPath;
        var mainClass = param.mainClass;
        var path = param.path;
        var type = param.type;
        
        var idx1 = stormStartScriptPath.indexOf(":");
        //ssh要连接的主机ip
        var ip = stormStartScriptPath.substring(0, idx1);
        var idx2 = stormStartScriptPath.indexOf("/");
        //ssh要连接的主机port
        var port = stormStartScriptPath.substring(idx1 + 1, idx2);
        //var idx3 = path.lastIndexOf("/");
        //ssh连接到主机的指定目录
        var baseDir = stormStartScriptPath.substring(idx2);
        console.log("baseDir: " + baseDir);

        var cmd = "cd "+ baseDir + "/dbus_jars;" + " ../bin/storm jar " + path + " " + mainClass + " -zk " + ConfUtils.zookeeperServers() + tid + type;
        console.log("cmd: " + cmd);
        var sshCmd = "ssh -p " + port + " " + user + "@" + ip + " " + "'" + cmd + "'";
        console.log("sshCmd: " + sshCmd);
        child_process.exec(sshCmd, function (err, stdout, stderr) {
            logger.info("err: " + err);
            logger.info("stderr: " + stderr);
            logger.info("stdout: " + stdout);
            if(err) {
                callback(err, err.message);
            } else {
                callback(null, stdout);
            }
        });
    },

    listTopologies: function (param) {
        return new Promise(function (resolve, reject) {
            stormApi.listTopologies(param).then(function (data) {
                resolve(data);
            }).catch(function (e) {
                reject(e);
            });
        });
    }
}

module.exports = router;