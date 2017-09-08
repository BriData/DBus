var config = require('../../config');
var request = require('request');
var $ = require("../utils/utils");
var ssh = require('../utils/ssh-client');
var logger = require('../utils/logger');

var baseDir = config.sshConfig.baseDir;
module.exports = {
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
            var dirsAndFiles = dirs[0].split("\n");
            var descFile = "tmp.txt";
            var flag = false;
            dirsAndFiles.forEach(function (e) {
               if(e == descFile)
               {
                   flag = true;
               }
            });

            var jarList = result;
            var paths = [];
            jarList.forEach(function (e) {
                paths.push(e.substring(0, e.lastIndexOf('/')));
            });
            var shell = " ";
            if(flag){
                shell = "rm tmp.txt;";
            }
            var index = 1;
            paths.forEach(function (e) {
                shell += " cat " + e + "/README >> tmp.txt; " + "echo " + index + " >> tmp.txt; ";
                index ++;
            });
            shell += " cat tmp.txt";
            ssh.exec(shell, null, function (err, stdout) {
                if(err) {
                    callback(err);
                }
                var data = [];
                data.push({num:index - 1,data:stdout,paths:paths,jarList:jarList});
                callback(null, data);
            });

        });
    },
    
     rename: function (params,callback) {
        logger.info("mv  " + baseDir + params["randomName"] + " " + params["originName"]);
        ssh.exec("mv  " + baseDir + params["randomName"] + " " + params["originName"] + ";ls", null, function (err, stdout) {
            if(err) {
                logger.info("err: " + err);
                callback(err);
            }
            var result = [];
            result.push(stdout);
            callback(null,result);
        });
    },
    modifyDesc: function (params,callback) {
        logger.info("cd  " + baseDir + params["path"] + ";  echo " +"\""+ params["description"] + "\"" +  " > README");
        ssh.exec("cd  " + baseDir + params["path"] + ";  echo " + "\""+ params["description"] + "\"" +  " > README;cat README", null, function (err, stdout) {
            if(err) {
                logger.info("err: " + err);
                callback(err);
            }
            var result = [];
            result.push(stdout);
            callback(null,result);
        });
    }
};
