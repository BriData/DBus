var express = require('express');
var router = express.Router();
var console = require('console');
var logger = require('../lib/utils/logger');
var ZooKeeper = require('node-zookeeper-client');
var config = require('../config');
var PropertiesReader = require('properties-reader');
var service = require('../lib/service/jar-manager-service-new');

var util = require('util');
var fs = require('fs');
var multiparty = require('multiparty');
var child_process = require('child_process');

router.get('/getJarList', function (req, res) {
    var client = ZooKeeper.createClient(config.zk.connect,{retries:3});
    client.once('connected', function () {
        path = "/DBus/Commons/global.properties";
        client.getData(path, function (error, data, stat) {
            if (error) {
                logger.error(error.stack);
                res.json({status: 500});
                client.close();
                return;
            }

            var data = data + "";
            var prop = PropertiesReader();
            prop.read(data);
            var params = {};
            params.path = prop.get('stormStartScriptPath');
            params.user = prop.get('user');

            // 如果带参数，则表明使用参数中的配置，目前用于全局配置页面的配置检测功能
            if(req.query.storm != null) params.path = req.query.storm;
            if(req.query.user != null) params.user = req.query.user;

            service.getJarList(params, function getJarList(err, data) {
                if (err) {
                    console.log(err);
                    res.json({status: -1, data: data});
                    return;
                }
                logger.info("getJarList data: " + JSON.stringify(data));
                
                var idx1 = params.path.indexOf(":");
                //ssh要连接的主机ip
                var ip = params.path.substring(0, idx1);
                var idx2 = params.path.indexOf("/");
                //ssh要连接的主机port
                var port = parseInt(params.path.substring(idx1 + 1, idx2));
                //ssh连接到主机的指定目录
                var baseDir = params.path.substring(idx2);

                var readmeInfoParams = {
                    port : port,
                    user : params.user,
                    ip : ip,
                    baseDir : baseDir
                };

                var methodsList = [];
                data.forEach(function (e) {
                    var path = e.val.substring(1, e.val.lastIndexOf("/"));
                    readmeInfoParams.path = path;
                    methodsList.push(service.getReadmeInfo(readmeInfoParams));
                });

                Promise.all(methodsList).then(function (resolve) {
                    try {
                        data.forEach(function (value, index, data) {
                            value.val = value.val.replace("readme.txt", resolve[index]);
                        });
                        res.json({status: 200, data: data});
                        client.close();
                    } catch (error) {
                        logger.error("Error occurred while process readmeInfo :%j.\n Error message -- %s \n%s", resolve, error.message, error.stack);
                        res.json({status: -1, error: error.message});
                    }
                });
            });
        });
    });
    client.connect();
});

router.get('/deleteJar', function (req, res) {
    try {
        var jarPath = req.query["path"].substring(1, req.query["path"].lastIndexOf("/"));
        var client = ZooKeeper.createClient(config.zk.connect,{retries:3});
        client.once('connected', function () {
            path = "/DBus/Commons/global.properties";
            client.getData(path, function (error, data, stat) {
                if (error) {
                    logger.error(error.stack);
                    res.json({status: 500});
                    client.close();
                    return;
                }

                var data = data + "";
                var prop = PropertiesReader();
                prop.read(data);
                var params = {};
                params.path = prop.get('stormStartScriptPath');
                params.user = prop.get('user');

                var idx1 = params.path.indexOf(":");
                //ssh要连接的主机ip
                var ip = params.path.substring(0, idx1);
                var idx2 = params.path.indexOf("/");
                //ssh要连接的主机port
                var port = parseInt(params.path.substring(idx1 + 1, idx2));
                //ssh连接到主机的指定目录
                var baseDir = params.path.substring(idx2);

                var p = {
                    port : port,
                    user : params.user,
                    ip : ip,
                    baseDir : baseDir,
                    path : jarPath
                };

                service.deleteJar(p, function deleteJar(err, data) {
                    if (err) {
                        console.log(err);
                        res.json({status: -1, data: data});
                        return;
                    }
                    // logger.info("deleteJar data: " + data);
                    res.json({status: 200, data: data});
                });
            });
        });
        client.connect();
    } catch (error){
        logger.error("Error occurred while process modify jar description :%j.\n Error message -- %s \n%s", "", error.message, error.stack);
    }
});


router.get('/modifyDesc', function (req, res) {
    try {
        var jarPath = req.query["path"].substring(1, req.query["path"].lastIndexOf("/"));
        var description = req.query["description"];
        var client = ZooKeeper.createClient(config.zk.connect,{retries:3});
        client.once('connected', function () {
            path = "/DBus/Commons/global.properties";
            client.getData(path, function (error, data, stat) {
                if (error) {
                    logger.error(error.stack);
                    res.json({status: 500});
                    client.close();
                    return;
                }

                var data = data + "";
                var prop = PropertiesReader();
                prop.read(data);
                var params = {};
                params.path = prop.get('stormStartScriptPath');
                params.user = prop.get('user');

                var idx1 = params.path.indexOf(":");
                //ssh要连接的主机ip
                var ip = params.path.substring(0, idx1);
                var idx2 = params.path.indexOf("/");
                //ssh要连接的主机port
                var port = parseInt(params.path.substring(idx1 + 1, idx2));
                //ssh连接到主机的指定目录
                var baseDir = params.path.substring(idx2);

                var p = {
                    port : port,
                    user : params.user,
                    ip : ip,
                    baseDir : baseDir,
                    path : jarPath,
                    description: description
                };

                service.modifyDesc(p, function modifyDesc(err, data) {
                    if (err) {
                        console.log(err);
                        res.json({status: -1, data: data});
                        return;
                    }
                    // logger.info("modifyDesc data: " + data);
                    res.json({status: 200, data: data});
                });
            });
        });
        client.connect();
    } catch (error){
        logger.error("Error occurred while process modify jar description :%j.\n Error message -- %s \n%s", "", error.message, error.stack);
    }
});


router.post('/upload', function(req, res) {
    var dir = __dirname + "/../files/";
    if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir);
    }
    //生成multiparty对象，并配置上传目标路径
    var form = new multiparty.Form({uploadDir: dir});
    //上传完成后处理
    form.parse(req, function(err, fields, files) {
        if(err){
            console.log('parse error: ' + err);
        } else {
            var jarUploadPath = fields.JarUploadPath[0];
            var inputFile = files.file[0];
			logger.info("JarUploadPath: " + fields.JarUploadPath[0]);
			logger.info("JarUploadPath fields: " + JSON.stringify(fields));
            logger.info("inputFile.path: " + inputFile.path);

            //windows用
            //TODO：注意Linux和windows的分隔符不一样
            // var index = inputFile.path.lastIndexOf('\\');
            //linux用
            var index = inputFile.path.lastIndexOf('/');
            var randomName = inputFile.path.substring(index + 1);
            var originName = inputFile.originalFilename;

            //windows用
            // var filePath = inputFile.path;
            // filePath = filePath.replace(/\\/g, "/");
            // filePath = filePath.replace(":", "");
            // filePath = "/cygdrive/" + filePath;
            //linux用
            var filePath = inputFile.path;
            try {
                var client = ZooKeeper.createClient(config.zk.connect,{retries:3});
                client.once('connected', function () {
                    path = "/DBus/Commons/global.properties";
                    client.getData(path, function (error, data, stat) {
                        if (error) {
                            logger.error(error.stack);
                            res.json({status: 500});
                            client.close();
                            return;
                        }

                        var data = data + "";
                        var prop = PropertiesReader();
                        prop.read(data);
                        var params = {};
                        params.path = prop.get('stormStartScriptPath');
                        params.user = prop.get('user');

                        var idx1 = params.path.indexOf(":");
                        //ssh要连接的主机ip
                        var ip = params.path.substring(0, idx1);
                        var idx2 = params.path.indexOf("/");
                        //ssh要连接的主机port
                        var port = parseInt(params.path.substring(idx1 + 1, idx2));
                        //ssh连接到主机的指定目录
                        var baseDir = params.path.substring(idx2);

                        var p = {
                            port: port,
                            user: params.user,
                            ip: ip,
                            baseDir: baseDir,
                            jarUploadPath: jarUploadPath,
                            randomName: randomName,
                            originName: originName,
                            filePath: filePath
                        };
						
						console.log("ppppppp: " + p);
						console.log("jarUploadPath: " + jarUploadPath);

                        service.createJarUploadCatalog(p, function createJarUploadCatalog(err, data) {
                            if (err) {
                                console.log(err);
                                res.json({status: -1, data: data});
                                return;
                            }
                            logger.info("createJarUploadCatalog data: " + data);
                            service.uploadJar(p, function uploadJar(err, data) {
                                if (err) {
                                    console.log(err);
                                    res.json({status: -1, data: data});
                                    return;
                                }
                                logger.info("uploadJar data: " + data);
                                service.renameJar(p, function renameJar(err, data) {
                                    if (err) {
                                        console.log(err);
                                        res.json({status: -1, data: data});
                                        return;
                                    }
                                    //删除文件
                                    // fs.unlink(dir + "\\" + randomName);
									fs.unlink(filePath);
                                    logger.info("renameJar data: " + data);
                                    res.json({status: 200, data: data});
                                });
                            });
                        });
                    });
                });
                client.connect();
            } catch (error){
                logger.error("Error occurred while process modify jar description :%j.\n Error message -- %s \n%s", "", error.message, error.stack);
            }
        }
    });
});

module.exports = router;
