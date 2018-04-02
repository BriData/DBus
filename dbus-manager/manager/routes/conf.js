/**
 * Created by dashencui on 2017/7/20.
 */
var express = require('express');
var router = express.Router();
var console = require('console');
var logger = require('../lib/utils/logger');
var Utils = require('../lib/utils/utils');
var conf = require('../lib/service/conf-service');
var ZooKeeper = require('node-zookeeper-client');
var config = require('../config');
var service = require('../lib/service/schema-service2');
var storm_service = require('../lib/service/storm_check');
var PropertiesReader = require('properties-reader');

router.get('/initialLoad', function (req, res) {
    var client = ZooKeeper.createClient(config.zk.connect,{retries:3});
    client.once('connected', function () {
        path_1 = "/DBus/Commons/global.properties";
        client.getData(path_1, function (error, data1, stat) {
            if (error) {
                logger.error(error.stack);
                res.json({status: 500});
                client.close();
                return;
            }
            path_2 = "/DBus/HeartBeat/Config/heartbeat_config.json";
            client.getData(path_2, function (error, data2, stat) {
                if (error) {
                    logger.error(error.stack);
                    res.json({status: 500});
                    client.close();
                    return;
                }
                var zookeeper = config.zk.connect;
                var data_total = {};
                var data1_j = data1+"";
                var prop = PropertiesReader();
                prop.read(data1_j);
                var global = {};
                global.bootstrap = prop.get('bootstrap.servers');
                global.zookeeper = zookeeper;
                global.monitor_url = prop.get('monitor_url');
                global.storm = prop.get('stormStartScriptPath');
                global.stormRest = prop.get('stormRest');
                _extractor = prop.get('stormStartExtractorScriptPath');
                global.user = prop.get('user');
                data_total.global_config = global;
                var data2_j = JSON.parse(data2);
                data_total.heartbeat_config = data2_j;
                res.json({data:data_total,status:200});
                client.close();
            });
        });
    });
    client.connect();
});

router.get('/stormcheck', function (req, res) {
    try {
        var path = req.query["path"];
        var params = {
            path:path,
        };
        storm_service.stormcheck(params, function startTopo(err, data) {
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

router.get('/search', function (req, res) {
    // var param = buildParam(req.query, ["dsId", "text", "pageSize", "pageNum"]);
    service.searchAll(function searchDataSchemas(err, response) {
        if(err) {
            res.json({status: 500, message: err.message});
        }
        res.json({status: 200, data: response.body});
    })
});

router.post('/savezk', function (req, res) {
    var param = req.body;
    var params = param.storm;
    var path_storm = params.substring(params.indexOf("/"),params.length);
    try {
        storm_service.stormcheck(param, function startTopo(err, data) {
            var idx = data.indexOf(path_storm);
            if (err) {
                console.log(err);
                res.json({status: 500});
                return;
            }
            else if(idx != -1){
                res.json({status: 500});
                return;
            }
            else {
                var prop1 = PropertiesReader();
                var param_str = JSON.stringify(param);
                prop1.read(param_str+"");
                prop1.set('bootstrap.servers',param.bootstrap);
                prop1.set('monitor_url',param.monitor_url);
                prop1.set('stormStartScriptPath',param.storm);
                prop1.set('user',param.user);
                prop1.set('stormRest',param.stormRest);
                var data_11 = 'bootstrap.servers='+prop1.getRaw('bootstrap.servers')+'\n'+'monitor_url='+prop1.getRaw('monitor_url')+'\n'
                    + 'stormStartScriptPath='+prop1.getRaw('stormStartScriptPath')+'\n' +'user='+prop1.getRaw('user')+'\n' +'stormRest='+prop1.getRaw('stormRest');
                var client = ZooKeeper.createClient(config.zk.connect,{retries:3});
                client.once('connected', function () {
                    logger.info('Connected to the server.');
                    var data = new Buffer(data_11);
                    path = "/DBus/Commons/global.properties";
                    client.exists(path, function (error, stat) {
                        if (error) {
                            logger.error(error.stack);
                            return;
                        }
                        if (stat) {
                            logger.info('Node exists.');
                        } else {
                            client.create(path, function (error) {
                                if (error) {
                                    logger.info('Failed to create node: %s due to: %s.', path, error);
                                } else {
                                    logger.info('Node: %s is successfully created.', path);
                                }
                                client.close();
                            });
                        }
                    });

                    client.setData(path, data, function (error, stat) {
                        if (error) {
                            logger.info(error.stack);
                            res.json({status: 500});
                            client.close();
                            return;
                        }
                        res.json({status: 200});
                        client.close();
                    });
                });
                client.connect();
            }
        });
    } catch (error){
        logger.error("Error occurred while process modify jar description :%j.\n Error message -- %s \n%s", "", error.message, error.stack);
    }
});

router.post('/save_heart_conf', function (req, res) {
    var param1  = req.body.data;
    var param_js = JSON.parse(param1);

    var param_str = JSON.stringify(param_js,null,'\t');
    var client = ZooKeeper.createClient(config.zk.connect,{retries:3});
    client.once('connected', function () {
        logger.info('Connected to the server.');
        var data = new Buffer(param_str);
        console.log(typeof(data));
        console.log(data);
        path = "/DBus/HeartBeat/Config/heartbeat_config.json";
        client.exists(path, function (error, stat) {
            if (error) {
                logger.error(error.stack);
                return;
            }
            if (stat) {
                logger.info('Node exists.');
            } else {
                logger.info('Node does not exist.'+'\n'+ 'creat a node');
                client.create(path, function (error) {
                    if (error) {
                        logger.info('Failed to create node: %s due to: %s.', path, error);
                    } else {
                        logger.info('Node: %s is successfully created.', path);
                    }
                });
            }
        });

        client.setData(path, data, function (error, stat) {
            if (error) {
                logger.error(error.stack);
                res.json({status: 500});
                return;
            }
            res.json({status: 200});
            client.close();
        });
    });
    client.connect();
    }
);

var buildParam = function (query, params) {
    var param = {};
    params.forEach(function (key) {
        if (query[key]) {
            param[key] = query[key];
        }
    });
    return param;
}

module.exports = router;