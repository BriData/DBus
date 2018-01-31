var express = require('express');
var router = express.Router();
var console = require('console');
var Utils = require('../lib/utils/utils');
var service = require('../lib/service/table-service');
var ZooKeeper = require ('node-zookeeper-client');
var config = require('../config');
var textEncoding = require('text-encoding');
var PropertiesReader = require('properties-reader');
var TextDecoder = textEncoding.TextDecoder;

router.get('/search', function (req, res) {
    var param = buildParam(req.query, ["dsID", "schemaName", "tableName", "pageSize", "pageNum"]);
    if (!param.pageSize) {
        param.pageSize = 12;
    }
    if (!param.pageNum) {
        param.pageNum = 1;
    }
    service.search(param, function searchDataTables(err, response) {
        if(err) {
            res.json({status: 500, message: err.message});
            return;
        }
        res.json({status: 200, data: response.body});
    })
});

router.get('/list', function (req, res) {
    var dsID = req.query.dsID;
    var schemaName = req.query.schemaName;
    if(!dsID) {
        logger.warn("parameter 'dsID' not found");
        res.json({status:404, message:"parameter 'dsID' not found"});
        return;
    }
    if(!schemaName) {
        logger.warn("parameter 'schemaName' not found");
        res.json({status:404, message:"parameter 'schemaName' not found"});
        return;
    }
    var param = buildParam(req.query, ["dsID","schemaName"]);
    service.list(param,function loadTables(err, response) {
        if(err) {
            res.json({status: 500, message: err.message});
        }
        res.json({status: 200, data: response.body});
    });
});

router.get('/updateTable',function(req,res){
    var param = buildParam(req.query,["id","physicalTableRegex","outputBeforeUpdateFlg","description","status"]);
    service.updateTable(param,function updateTable(err, response) {
            console.log(response.body);
            if(err) {
                res.json({status: 500, message: err.message});
                return;
            }
            res.json({status: 200, data: response.body});
        }
    );
});

router.get('/readTableVersion',function(req,res){
    var param = buildParam(req.query,["dsName","schemaName","tableName"]);
    var client = ZooKeeper.createClient(config.zk.connect,config.zk.client);
    client.once('connected', function() {
        console.log('Connected to the server.');
      client.getChildren("/DBus/FullPuller/"+param["dsName"]+"/"+param["schemaName"]+"/"+param["tableName"],function (error, children,stat) {
        if (error) {
            console.log(error.stack);
            res.json({status: 500});
            return;
        }
        res.json({status: 200, data: children});
      });
        client.close();
    });
    client.connect();
});

router.get('/readVersionData',function(req,res){
    var param = buildParam(req.query,["path","version"]);
    var client = ZooKeeper.createClient(config.zk.connect,config.zk.client);
    client.once('connected', function() {
        console.log('Connected to the server.');
        client.getData(param["path"] + "/" + param["version"], function (event) {
            console.log("event: " + event);
        },function (error,data ,stat) {
            if (error) {
                console.log(error.stack);
                res.json({status: 500});
                return;
            }
            if(!data){
                res.json({status: 500});
                return;
            }
            var string = new TextDecoder("utf-8").decode(data);
            res.json({status: 200, data: string});
        });
        client.close();
    });
    client.connect();
});

router.get('/pullWhole',function(req,res){
    var param = req.query;
    service.pullWhole(param,function pull(err, response) {
            if(err) {
                res.json({status: 500, message: err.message});
                return;
            }
            if(response.body.status == 0){
                res.json({status: 200, data: response.body});
            }else{
                res.json({status: 500, data: response.body});
            }

        }
    );
});

router.get('/pullIncrement',function(req,res){
    var param = buildParam(req.query,["id","dsId","dsName","schemaName","tableName","status","physicalTableRegex","outputTopic","version","namespace","createTime","type"]);
    if(param["status"] == "abort"){
        param["version"] = 0;
    }
    service.pullIncrement(param,function pull(err, response) {
            console.log(response.body);
            if(err) {
                res.json({status: 500, message: err.message});
                return;
            }
        if(response.body.status == 0){
            res.json({status: 200, data: response.body});
        }else{
            res.json({status: 500, data: response.body});
        }
        }
    );
});

router.get('/executeSql', function (req, res) {
    var param = buildParam(req.query, ["dsId","dsType","URL","user","password","sql"]);
    service.getPassword(param.dsId,function(err,response) {
        param.password = JSON.parse(response.body).dbusPassword;
        
        service.executeSql(param, function sqlResult(err, response) {
            if (err) {
                res.json({status: 500, message: err.message});
                return;
            }
            res.json({status: 200, data: response.body});
        });


    });
});

router.get('/stop',function(req,res){
    var param = buildParam(req.query,["id"]);
    service.stop(param,function stop(err, response) {
            console.log(response.body);
            if(err) {
                res.json({status: 500, message: err.message});
                return;
            }
            res.json({status: 200, data: response.body});
        }
    );
});

router.get('/confirmStatusChange',function(req,res){
    var param = buildParam(req.query,["tableId"]);
    service.confirmStatusChange(param, function (err, response) {
            if(err) {
                res.json({status: 500, message: err.message});
                return;
            }
            res.json({status: 200, data: response.body});
        }
    );
});

router.get('/getVersionListByTableId',function(req,res){
    var param = buildParam(req.query,["tableId"]);
    service.getVersionListByTableId(param, function (err, response) {
            if(err) {
                res.json({status: 500, message: err.message});
                return;
            }
            res.json({status: 200, data: response.body});
        }
    );
});

router.get('/getVersionDetail',function(req,res){
    var param = req.query;
    service.getVersionDetail(param, function (err, response) {
            if(err) {
                res.json({status: 500, message: err.message});
                return;
            }
            res.json({status: 200, data: response.body});
        }
    );
});

router.get('/desensitization',function(req,res){
    var param = buildParam(req.query,["tableId"]);
    service.desensitization(param, function (err, response) {
            if(err) {
                res.json({status: 500, message: err.message});
                return;
            }
            res.json({status: 200, data: response.body});
        }
    );
});

router.get('/fetchTableColumns',function(req,res){
    var param = buildParam(req.query,["tableId"]);
    service.fetchTableColumns(param, function (err, response) {
            if(err) {
                res.json({status: 500, message: err.message});
                return;
            }
            res.json({status: 200, data: response.body});
        }
    );
});

//获取脱敏类型
router.get('/fetchEncodeAlgorithms', function (req, res) {
    var client = ZooKeeper.createClient(config.zk.connect,{retries:3});
    client.once('connected', function () {
        var path = "/DBus/Commons/encoderPlugins";
        client.getData(path, function (error, data, stat) {
            if (error) {
                logger.error(error.stack);
                res.json({status: 500});
                client.close();
                return;
            }

            var result = {};
            var prop = PropertiesReader();
            prop.read(data);

            prop.each((key, value) => {
                if(key == 'BuiltInEncodeType') {
                    //拆分内置加密类型
                    var encryptedTypeArr = value.split(',');
                    for(var i in encryptedTypeArr) {
                        result["\"" + encryptedTypeArr[i] + "\""] = encryptedTypeArr[i];
                    }
                } else {
                    result["\"" + value + "\""] = value;
                }
            })
            res.json({data:result, status:200});
            client.close();
        });
    });
    client.connect();
});

// router.get('/fetchEncodeAlgorithms',function(req,res){
//     service.fetchEncodeAlgorithms(function (err, response) {
//             if(err) {
//                 res.json({status: 500, message: err.message});
//                 return;
//             }
//             res.json({status: 200, data: response.body});
//         }
//     );
// });

router.get('/changeDesensitization',function(req,res){
    var param = req.query;
    service.changeDesensitization(param, function (err, response) {
            if(err) {
                res.json({status: 500, message: err.message});
                return;
            }
            res.json({status: 200, data: response.body});
        }
    );
});

router.get('/active', function (req, res) {
    var param = buildParam(req.query, [ "id", "status"]);
    service.active(param,function activeDs(err,response) {
        console.log(response.body);
        if(err) {
            res.json({status: 500, message: err.message});
            return;
        }
        res.json({status: 200, data: response.body});
    });
});

router.get('/inactive', function (req, res) {
    var param = buildParam(req.query, [ "id", "status"]);
    service.inactive(param,function inactiveDs(err,response) {
        console.log(response.body);
        if(err) {
            res.json({status: 500, message: err.message});
            return;
        }
        res.json({status: 200, data: response.body});
    });
});

var buildParam = function (query, params) {
    var param = {};
    params.forEach(function (key) {
        if (query[key]) {
            param[key] = query[key];
        }
    });
    return param;
}

router.get('/listTable', function (req, res) {
    var dsName = req.query["dsName"];
    var schemaName = req.query["schemaName"];
    if(!dsName) {
        logger.warn("parameter 'dsName' not found");
        res.json({status:404, message:"parameter 'dsName' not found"});
        return;
    }
    if(!schemaName) {
        logger.warn("parameter 'schemaName' not found");
        res.json({status:404, message:"parameter 'schemaName' not found"});
        return;
    }

    service.listTable(dsName, schemaName, function loadTables(err, response) {
        var resultList = JSON.parse(response.body);
        var list = [];
        for(var i = 0; i < resultList.length; i++) {
            var tableInfo = {};
            Utils.extend(tableInfo, resultList[i], ["tableName", "physicalTableRegex","outputTopic"]);
            list.push(tableInfo);
        }
        res.json({status: 200, data: list});
    });
});

router.get('/deleteTable', function (req, res) {
    var param = req.query;
    service.deleteTable(param, function (err, response) {
        if (err) {
            res.json({status: 500, message: err.message});
        } else {
            res.json({status: 200, data: response.body});
        }
    });
});



// 规则组开始
router.get('/getAllRuleGroup', function (req, res) {
    var param = req.query;
    service.getAllRuleGroup(param, function (err, response) {
        if (err) {
            res.json({status: 500, message: err.message});
        } else {
            res.json({status: 200, data: response.body});
        }
    });
});

router.get('/updateRuleGroup', function (req, res) {
    var param = req.query;
    service.updateRuleGroup(param, function (err, response) {
        if (err) {
            res.json({status: 500, message: err.message});
        } else {
            res.json({status: 200, data: response.body});
        }
    });
});

router.get('/deleteRuleGroup', function (req, res) {
    var param = req.query;
    service.deleteRuleGroup(param, function (err, response) {
        if (err) {
            res.json({status: 500, message: err.message});
        } else {
            res.json({status: 200, data: response.body});
        }
    });
});

router.get('/addGroup', function (req, res) {
    var param = req.query;
    service.addGroup(param, function (err, response) {
        if (err) {
            res.json({status: 500, message: err.message});
        } else {
            res.json({status: 200, data: response.body});
        }
    });
});

router.get('/cloneRuleGroup', function (req, res) {
    var param = req.query;
    service.cloneRuleGroup(param, function (err, response) {
        if (err) {
            res.json({status: 500, message: err.message});
        } else {
            res.json({status: 200, data: response.body});
        }
    });
});


router.get('/upgradeVersion', function (req, res) {
    var param = req.query;
    service.upgradeVersion(param, function (err, response) {
        if (err) {
            if(response.statusCode == 202) res.json({status: 500, message: response.body.message});
            else res.json({status: 500, message: err.message});
        } else {
            res.json({status: 200, data: response.body});
        }
    });
});

router.get('/diffGroupRule', function (req, res) {
    var param = req.query;
    service.diffGroupRule(param, function (err, response) {
        if (err) {
            res.json({status: 500, message: err.message});
        } else {
            res.json({status: 200, data: response.body});
        }
    });
});

// 规则组结束

// 规则开始
router.get('/getAllRules', function (req, res) {
    var param = req.query;
    service.getAllRules(param, function (err, response) {
        if (err) {
            res.json({status: 500, message: err.message});
        } else {
            res.json({status: 200, data: response.body});
        }
    });
});

router.post('/saveAllRules', function (req, res) {
    var param = req.body;
    service.saveAllRules(param, function (err, response) {
        if (err) {
            res.json({status: 500, message: err.message});
        } else {
            res.json({status: 200, data: response.body});
        }
    });
});

router.post('/executeRules', function (req, res) {
    var param = req.body;
    service.executeRules(param, function (err, response) {
        if (err) {
            res.json({status: 500, message: err.message});
        } else {
            res.json({status: 200, data: response.body});
        }
    });
});
// 规则结束

module.exports = router;
