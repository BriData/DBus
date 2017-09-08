var express = require('express');
var router = express.Router();
var Utils = require('../lib/utils/utils');
var service = require('../lib/service/schema-service');
var logger = require('../lib/utils/logger');

router.get('/list', function (req, res) {
    var dsId = req.query.dsId;
    if(!dsId) {
        logger.warn("parameter 'dsId' not found");
        res.json({status:404, message:"parameter 'dsId' not found"});
        return;
    }
    service.list(dsId, function loadSchemas(err, response) {
        if(err) {
            res.json({status: 500, message: err.message});
        }
        var resultList = JSON.parse(response.body);
        var list = [];
        for(var i = 0; i < resultList.length; i++) {
            var ds = {};
            Utils.extend(ds, resultList[i], ["id", "schemaName"]);
            list.push(ds);
        }
        res.json({status: 200, data: list});
    });
});

router.get('/listByDsName', function (req, res) {
    var dsName = req.query["dsName"];
    if(!dsName) {
        logger.warn("parameter 'dsId' not found");
        res.json({status:404, message:"parameter 'dsId' not found"});
        return;
    }
    service.listByDsName(dsName, function loadSchemas(err, response) {
        if(err) {
            res.json({status: 500, message: err.message});
            return;
        }
        var resultList = JSON.parse(response.body);
        var list = [];
        for(var i = 0; i < resultList.length; i++) {
            var ds = {};
            Utils.extend(ds, resultList[i], [ "schemaName","status","srcTopic","targetTopic"]);
            list.push(ds);
        }
        
        res.json({status: 200, data: list});
    });
});


router.get('/checkManagerSchema', function (req, res) {
    var dsId = parseInt(req.query["dsId"]);
    var schemaName = req.query["schemaName"];
    if(!dsId) {
        logger.warn("parameter 'dsId' not found");
        res.json({status:404, message:"parameter 'dsId' not found"});
        return;
    }
    if(!schemaName) {
        logger.warn("parameter 'schemaName' not found");
        res.json({status:404, message:"parameter 'schemaName' not found"});
        return;
    }
    var param = {
        dsId:dsId,
        schemaName:schemaName
    }
    service.listBySchemaName(param, function loadSchemas(err, response) {
        if(err) {
            res.json({status: 500, message: err.message});
            return;
        }
        var data = [];
        var schemaList = JSON.parse(response.body);
        var len =  schemaList.length;
        if(len != 0)
        {
            for(var i in schemaList)
            {
                if(dsId == schemaList[i].dsId && schemaName == schemaList[i].schemaName)
                {
                    data.push({status:schemaList[i].status,srcTopic:schemaList[i].srcTopic,targetTopic:schemaList[i].targetTopic,description:schemaList[i].description});
                    break;
                }
            }
        }
        res.json({status: 200, data: data});
    });
});


router.get('/listStatus', function (req, res) {
    var schemaName = req.query["schemaName"];
    if(!schemaName) {
        logger.warn("parameter 'schemaName' not found");
        res.json({status:404, message:"parameter 'schemaName' not found"});
        return;
    }
    service.listStatus(schemaName, function loadSchemaStatus(err, response) {
        if(err) {
            res.json({status: 500, message: err.message});
            return;
        }
        var resultList = JSON.parse(response.body);
        var list = [];
        for(var i = 0; i < resultList.length; i++) {
            var schemaInfo = {};
            Utils.extend(schemaInfo, resultList[i], ["status", "srcTopic","targetTopic"]);
            list.push(schemaInfo);
        }
        res.json({status: 200, data: list});
    });
});


module.exports = router;
