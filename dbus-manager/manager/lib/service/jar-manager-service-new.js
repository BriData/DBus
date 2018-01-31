var config = require('../../config');
var request = require('request');
var ssh = require('../utils/ssh-client');
var logger = require('../utils/logger');
var child_process = require('child_process');

const jarCatalog = "/dbus_jars";

module.exports = {
    getJarList: function (params, callback) {
        var user = params.user;
        var path = params.path;

        var idx1 = path.indexOf(":");
        //ssh要连接的主机ip
        var ip = path.substring(0, idx1);
        var idx2 = path.indexOf("/");
        //ssh要连接的主机port
        var port = parseInt(path.substring(idx1 + 1, idx2));
        //ssh连接到主机的指定目录
        var baseDir = path.substring(idx2);
        logger.info("getJarList baseDir: " + baseDir);

        var cmd = "cd " + baseDir + jarCatalog + "; ls -ARmF ";
        logger.info("getJarList cmd: " + cmd);

        var sshCmd = "ssh -p " + port + " " + user + "@" + ip + " " + "'" + cmd + "'";
        logger.info("getJarList sshCmd: " + sshCmd);

        child_process.exec(sshCmd, function (err, stdout, stderr) {
            if(err) {
                logger.info("getJarList err result: " + err);
                callback(err, stderr);
            } else if(stderr) {
                var result = [];
                result.push(stdout);
                logger.info("getJarList stderr result: " + stderr);
                callback(null, result);
            } else {
                logger.info("初始值，list jar info: " + stdout);
                stdout += "\n";
                // stdout = stdout.replace(", dbus_startTopology.sh*", "");
                //从dbus_jars中获取的数据
                const doubleWrap = "\n\n";
                var level_1_str = stdout.substring(3, stdout.indexOf(doubleWrap));
                var level_1_array_copy = level_1_str.split(",");
                stdout = stdout.replace(".:\n" + level_1_str + doubleWrap, "");

                var index = 0;
                var level_1_array = [];
                for(var i = 0; i < level_1_array_copy.length; i++) {
                    if(level_1_array_copy[i].lastIndexOf("x/") != -1) {
                        level_1_array[index++] = level_1_array_copy[i];
                    }
                }

                var level_2_array = [];
                level_1_array.forEach(function (value, index, level_1_array) {
                    var convertValue = "./" + value.trim().replace("/",":");
                    var idx = stdout.indexOf(convertValue) + convertValue.length + 1;
                    var tmpStr = stdout.substring(idx, stdout.length);
                    if(stdout.charAt(idx) != "\n") {
                        var str = tmpStr.substring(0, tmpStr.indexOf(doubleWrap));
                        level_2_array[index] = str;
                        stdout = stdout.replace(convertValue + "\n" + level_2_array[index] + doubleWrap, "");
                    } else {
                        stdout = stdout.replace(convertValue + "\n\n" + doubleWrap, "");
                    }
                });

                var level_3_array_total = [];
                level_2_array.forEach(function (value, index, level_2_array) {
                    var level_2_array_sub_str =  value.split(",");
                    var level_3_array = [];
                    level_2_array_sub_str.forEach(function (value, subIndex, level_2_array_sub_str) {
                        var convertValue = "./" + level_1_array[index].trim() + value.trim().replace("/",":");
                        var idx = stdout.indexOf(convertValue) + convertValue.length + 1;
                        if(stdout.charAt(idx) != "\n") {
                            var tmpStr = stdout.substring(idx, stdout.length);
                            var str = tmpStr.substring(0, tmpStr.indexOf(doubleWrap));
                            var strArr = str.split(",");
                            var strConcat = "";
                            strArr.forEach(function (val, idx, strArr) {
                                strConcat = strConcat + "./" + level_1_array[index].trim() + value.trim() + val.trim() + ",";
                            });
                            level_3_array[subIndex] = strConcat;
                            stdout = stdout.replace(convertValue + "\n" + str + doubleWrap, "");
                        } else {
                            stdout = stdout.replace(convertValue + "\n\n" + doubleWrap, "");
                        }
                    });
                    level_3_array_total[index] = level_3_array;
                });

                var level_4_array = [];
                level_3_array_total.forEach(function (value, index, level_3_array_total) {
                    value.forEach(function (v, i, initValue) {
                        var level_3_array_sub_str = v.substring(0,v.length - 1).split(",");
                        level_3_array_sub_str.forEach(function (val, subIndex, level_3_array_sub_str) {
                            var convertValue = val.trim().substring(0,val.trim().length - 1) + ":";
                            var idx = stdout.indexOf(convertValue) + convertValue.length + 1;
                            if(stdout.charAt(idx) != "\n") {
                                var tmpStr = stdout.substring(idx, stdout.length);
                                var str = tmpStr.substring(0, tmpStr.indexOf(doubleWrap));
                                level_4_array.push({val:val + str});
                                stdout = stdout.replace(convertValue + "\n" + str + doubleWrap, "");
                            } else {
                                stdout = stdout.replace(convertValue + "\n\n" + doubleWrap, "");
                            }
                        })
                    })

                });
                logger.info("最终结果是：level_4_array: " + JSON.stringify(level_4_array));
                logger.info("操作完成时的stdout: " + stdout);
                callback(null, level_4_array);
            }
        });
    },


    getReadmeInfo: function (params) {
        return new Promise(function (resolve, reject) {
            var port = params.port;
            var user = params.user;
            var ip = params.ip;
            var baseDir = params.baseDir;
            var path = params.path;

            var cmd = "cd " + baseDir + jarCatalog + path + "; cat readme.txt ";
            logger.info("getReadmeInfo cmd: " + cmd);
            var sshCmd = "ssh -p " + port + " " + user + "@" + ip + " " + "'" + cmd + "'";
            logger.info("getReadmeInfo sshCmd: " + sshCmd);
            child_process.exec(sshCmd, function (err, stdout, stderr) {
                if(err) {
                    logger.error("getReadmeInfo err result: " + err);
                    var result = [];
                    result.push(err);
                    // reject({err:err});
                    resolve(result);
                } else if(stderr) {
                    var result = [];
                    result.push(stderr);
                    resolve(result);
                    logger.info("getReadmeInfo stderr result: " + stderr);
                } else {
                    var result = [];
                    result.push(stdout);
                    resolve(result);
                    logger.info("getReadmeInfo stdout result: " + stdout);
                }
            });

            console.log("sad");
        })
    },

    deleteJar: function (params, callback) {
        var port = params.port;
        var user = params.user;
        var ip = params.ip;
        var baseDir = params.baseDir;
        var path = params.path;
        var prefixPath = path.substring(0, path.substring(0, path.length - 1).lastIndexOf("/"));
        var paths = path.substring(1, path.length).split("/");
        var JarFolder = paths[2];

        var cmd = "cd " + baseDir + jarCatalog + prefixPath + "; rm -R " + JarFolder;
        logger.info("cmd: " + cmd);
        var sshCmd = "ssh -p " + port + " " + user + "@" + ip + " " + "'" + cmd + "'";
        logger.info("sshCmd: " + sshCmd);
        child_process.exec(sshCmd, function (err, stdout, stderr) {
            if(err) {
                logger.error("deleteJar err result: " + err);
                callback(null, err)
            } else if(stderr) {
                logger.info("deleteJar stderr result: " + stderr);
                callback(null, stderr)
            } else {
                logger.info("deleteJar stdout result: " + stdout);
                callback(null, stdout)
            }
        });
    },

    modifyDesc: function (params, callback) {
        var port = params.port;
        var user = params.user;
        var ip = params.ip;
        var baseDir = params.baseDir;
        var path = params.path;
        var description = params.description.toString();

        var cmd = "cd " + baseDir + jarCatalog  + path + "; touch readme.txt; echo -e " + "\"" + description + "\"" + " > readme.txt";
        logger.info("cmd: " + cmd);
        var sshCmd = "ssh -p " + port + " " + user + "@" + ip + " " + "\"" + cmd + "\"";
        logger.info("sshCmd: " + sshCmd);
        child_process.exec(sshCmd, function (err, stdout, stderr) {
            if(err) {
                logger.error("modifyDesc err result: " + err);
                callback(null, err);
            } else if(stderr) {
                logger.info("modifyDesc stderr result: " + stderr);
                callback(null, stderr);
            } else {
                logger.info("modifyDesc stdout result: " + stdout);
                callback(null, stdout);
            }
        });
    },

    uploadJar: function (params, callback) {
        var port = params.port;
        var user = params.user;
        var ip = params.ip;
        var baseDir = params.baseDir;
        var jarUploadPath = params.jarUploadPath.substring(1, params.jarUploadPath.length);
        var filePath = params.filePath;
        var target_dir = user + "@" + ip + ":" + baseDir + jarCatalog + jarUploadPath;
        var cmd = "scp -P "+ port +" " + filePath + " " + target_dir;
        logger.info("cmd: " + cmd);
        child_process.exec(cmd, function (err, stdout, stderr) {
            if(err) {
                logger.error("uploadJar err result: " + err);
                callback(null, err);
            } else if(stderr) {
                logger.info("uploadJar stderr result: " + stderr);
                callback(null, stderr);
            } else {
                logger.info("uploadJar stdout result: " + stdout);
                callback(null, stdout);
            }
        });
    },

    createJarUploadCatalog: function (params, callback) {
        var port = params.port;
        var user = params.user;
        var ip = params.ip;
        var baseDir = params.baseDir;
        var cmd = "cd " + baseDir + jarCatalog + "; mkdir -pv " + params.jarUploadPath + "; cd " + params.jarUploadPath + "; touch readme.txt";
        logger.info("cmd: " + cmd);
        var sshCmd = "ssh -p " + port + " " + user + "@" + ip + " " + "\"" + cmd + "\"";
        logger.info("sshCmd: " + sshCmd);
        child_process.exec(sshCmd, function (err, stdout, stderr) {
            if(err) {
                logger.error("createJarUploadCatalog err result: " + err);
                callback(null, err);
            } else if(stderr) {
                logger.info("createJarUploadCatalog stderr result: " + stderr);
                callback(null, stderr);
            } else {
                logger.info("createJarUploadCatalog stdout result: " + stdout);
                callback(null, stdout);
            }
        });
    },

    renameJar: function (params, callback) {
        var port = params.port;
        var user = params.user;
        var ip = params.ip;
        var baseDir = params.baseDir;
        var jarUploadPath = params.jarUploadPath.substring(1, params.jarUploadPath.length);
        var randomName = params.randomName;
        var originName = params.originName;
        var cmd = "cd " + baseDir + jarCatalog + jarUploadPath + "; mv " + randomName + " " + originName;
        logger.info("cmd: " + cmd);
        var sshCmd = "ssh -p " + port + " " + user + "@" + ip + " " + "\"" + cmd + "\"";
        logger.info("sshCmd: " + sshCmd);
        child_process.exec(sshCmd, function (err, stdout, stderr) {
            if(err) {
                logger.error("uploadJar err result: " + err);
                callback(null, err);
            } else if(stderr) {
                logger.info("uploadJar stderr result: " + stderr);
                callback(null, stderr);
            } else {
                logger.info("uploadJar stdout result: " + stdout);
                callback(null, stdout);
            }
        });
    }


};
