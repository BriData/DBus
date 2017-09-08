
var express = require('express');
var router = express.Router();
var console = require('console');
var logger = require('../lib/utils/logger');
var ZooKeeper = require('node-zookeeper-client');
var config = require('../config');
var PropertiesReader = require('properties-reader');
var service = require('../lib/service/start-topology-service');

router.get('/getPath', function (req, res) {
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

            var result = {};
            var data = data + "";
            var prop = PropertiesReader();
            prop.read(data);
            var global = {};
            global.stormStartScriptPath = prop.get('stormStartScriptPath');
            global.user = prop.get('user');
            console.log("global: " + global);
            result.global_config = global;
            res.json({data:result, status:200});
            client.close();
        });
    });
    client.connect();
});

router.get('/startTopo', function (req, res) {
    try {
        var path = req.query["path"];
        var dsName = req.query["dsName"];
        var topologyType = req.query["topologyType"];
        var user = req.query["user"];
        var params = {
            user:user,
            path:path,
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
