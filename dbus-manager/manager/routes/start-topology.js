
var express = require('express');
var router = express.Router();
var console = require('console');
var logger = require('../lib/utils/logger');
var ZooKeeper = require('node-zookeeper-client');
var config = require('../config');
var PropertiesReader = require('properties-reader');
var service = require('../lib/service/start-topology-service');
var jarManagerService = require('../lib/service/jar-manager-service-new');

router.get('/getPath', function (req, res) {
    var client = ZooKeeper.createClient(config.zk.connect,{retries:3});
    client.once('connected', function () {
        var path = "/DBus/Commons/global.properties";
        client.getData(path, function (error, data, stat) {
            if (error) {
                logger.error(error.stack);
                res.json({status: 500});
                client.close();
                return;
            }

            var result = {};
            var data = data + "";
            var prop = PropertiesReader();
            prop.read(data);
            var global = {};
            global.stormStartScriptPath = prop.get('stormStartScriptPath');
            global.path = prop.get('stormStartScriptPath');
            global.user = prop.get('user');
            console.log("global: " + global);

            jarManagerService.getJarList(global, function getJarList(err, data) {
                if (err) {
                    console.log(err);
                    res.json({status: -1, data: data});
                    return;
                }
                logger.info("getJarList data: " + JSON.stringify(data));
                var len = data.length;
                var highestVersion = data[len - 1].val.split("/")[1];
                var dispatcherAppenderHighVerPath = "";
                var splitterPullerHighVerPath = "";
                var logProcessorHighVerPath = "";
                var extractorHighVerPath = "";

                for(var i = len - 1; i >= 0; i--) {
                    if(data[i].val.indexOf(highestVersion + "/dispatcher_appender") != -1) {
                        dispatcherAppenderHighVerPath = data[i].val;
                        break;
                    }
                }

                for(var i = len - 1; i >= 0; i--) {
                    if(data[i].val.indexOf(highestVersion + "/splitter_puller") != -1) {
                        splitterPullerHighVerPath = data[i].val;
                        break;
                    }
                }

                for(var i = len - 1; i >= 0; i--) {
                    if(data[i].val.indexOf(highestVersion + "/log_processor") != -1) {
                        logProcessorHighVerPath = data[i].val;
                        break;
                    }
                }

                for(var i = len - 1; i >= 0; i--) {
                    if(data[i].val.indexOf(highestVersion + "/extractor") != -1) {
                        extractorHighVerPath = data[i].val;
                        break;
                    }
                }

                global.dispatcherAppenderHighVerPath = dispatcherAppenderHighVerPath;
                global.splitterPullerHighVerPath = splitterPullerHighVerPath;
                global.logProcessorHighVerPath = logProcessorHighVerPath;
                global.extractorHighVerPath = extractorHighVerPath;
                result.global_config = global;
                res.json({data:result, status:200});
                client.close();
            });
            
        });
    });
    client.connect();
});

router.get('/startTopo', function (req, res) {
    try {
        var dsName = req.query["dsName"];
        var stormPath = req.query["stormPath"];
        var jarPath = req.query["jarPath"];
        var jarName = req.query["jarName"];
        var topologyType = req.query["topologyType"];
        var user = req.query["user"];
        var params = {
            user:user,
            stormPath:stormPath,
            jarPath:jarPath,
            jarName:jarName,
            dsName:dsName,
            topologyType:topologyType
        };
        service.startTopo(params, function startTopo(err, data) {
            if (err) {
                console.log(err);
                res.json({status: -1, data: data});
                return;
            }
            console.log("data: " + data);
            res.json({status: 200, data: data});
        });
    } catch (error){
        logger.error("Error occurred while process modify jar description :%j.\n Error message -- %s \n%s", "", error.message, error.stack);
    }
});

module.exports = router;
