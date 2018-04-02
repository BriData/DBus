var express = require('express');
var router = express.Router();
var service = require('../lib/service/schema-table-insert-service');
var SchemaService = require('../lib/service/schema-service2');
var TableService = require('../lib/service/table-service');
var logger = require('../lib/utils/logger');
var util = require('util');

//将要插入管理库的table信息，同时存入源库中。
router.post('/insertTable', function (req, res) {
    // var tables = req.query["sourceTable"];
    var tables = JSON.parse(req.body["sourceTable"]);
    var dsType = req.body["dsType"];
    //如果tables非空，则执行以下逻辑。
    if(typeof (tables) != "undefined" && dsType != "mysql")
    {
        var param = {
            dsName: tables[0]["dsName"]
        };
        service.listSourceTables(param, function listTables(err, response) {
            if(err) {
                res.json({status: 500, message: err.message});
                return;
            }
            var tableList = JSON.parse(response.body);
            var tablesInfo = [];
            for(var index in tableList) {
                tablesInfo.push({schemaName:tableList[index].schemaName,tableName:tableList[index].tableName});
            }
            var len = tables.length - 1;
            for (var e in tables) {
                var schemaName = tables[e].schemaName;
                var tableName = tables[e].tableName;
                var flag = true;
                for(var j in tableList)
                {
                    //如果参数一致，表示table信息已经录入，将flag置为false。
                    if(schemaName == tableList[j].schemaName && tableName == tableList[j].tableName)
                    {
                        flag = false;
                    }
                }
                //flag为true，表示该table信息还未录入，将table信息插入到源库中。
                if(flag)
                {
                    service.insertDbusTables(tables[e], function insertDbusTables(err, response) {
                        if(err) {
                            res.json({status: 500, message: err.message});
                            return;
                        }
                    });
                }
                //判断是否所有表操作完成，完成即可退出。
                if(e == len)
                {
                    res.json({status: 200, data: "OK"});
                }
            }
        });
    }//tables为空，直接返回即可。
    else{
        res.json({status: 200, data: "OK"});
        return;
    }
});

router.post('/insert', function (req, res) {
    var dsId = parseInt(req.body["dsId"]);
    var dsType = req.body["dsType"];
    var dsName = req.body["dsName"];
    var schemaName = req.body["schemaName"]|| 0;
    var description = req.body["description"];
    var status = req.body["status"];
    var src_topic = req.body["src_topic"];
    var target_topic = req.body["target_topic"];
    var tables = JSON.parse(req.body["tables"]);

    if(schemaName == 0)
    {
        res.json({status: 200, data: "OK"});
        return;
    }
    var param = {
        dsType: dsType,
        dsId: dsId,
        dsName: dsName,
        schemaName: schemaName,
        description: description,
        status: status,
        srcTopic: src_topic,
        targetTopic: target_topic
    };
    helper.listSchemaId(param).then(function (data) {
        var schemaId1 = data.schemaId1;
        var schemaId2 = data.schemaId2;
        var dsType = param.dsType;
        var schemaInsertMethods = [];
        var schemaName = param.schemaName;
        var flag = true;

        if("mysql" == dsType) {
             flag = "dbus" != schemaName.toString();
        }
        else {
             flag = "DBUS" != schemaName.toString();
        }


        if(schemaId1 == -1 && flag) {
            schemaInsertMethods.push(service.insertSchema(param));
        }
        if(schemaId2 == -1) {
            if("mysql" == dsType) {
                var srcTopic = param.dsName + ".dbus";
                var targetTopic = param.dsName + ".dbus" + ".result";
                var schemaParam = {
                    dsId: param.dsId,
                    schemaName: "dbus",
                    description: "",
                    status: "active",
                    srcTopic: srcTopic,
                    targetTopic: targetTopic
                };
                schemaInsertMethods.push(service.insertSchema(schemaParam));
            } else {
                var srcTopic = param.dsName + ".DBUS";
                var targetTopic = param.dsName + ".DBUS" + ".result";
                var schemaParam = {
                    dsId: param.dsId,
                    schemaName: "DBUS",
                    description: "",
                    status: "active",
                    srcTopic: srcTopic,
                    targetTopic: targetTopic
                };
                schemaInsertMethods.push(service.insertSchema(schemaParam));
            }
        }
        if(schemaId1 == -1 || schemaId2 == -1) {
            Promise.all(schemaInsertMethods).then(function (dataList) {
                try {
                    var length = dataList.length;
                    if(length == 1) {
                        if(schemaId1 == -1) {
                            schemaId1 = JSON.parse(dataList[0]);
                        }

                        if (schemaId2 == -1 ) {
                            schemaId2 = JSON.parse(dataList[0]);
                        }
                    } else if(length == 2) {
                        schemaId1 = JSON.parse(dataList[0]);
                        schemaId2 = JSON.parse(dataList[1]);
                    } else {
                        return;
                    }
                
                    //表信息，保存了所有需要插入的表信息，如果非空，则执行以下逻辑。
                    if(typeof (tables) != "undefined")
                    {
                        var tableParams = [];
                        for (var e in tables) {
                            var tName = tables[e].tableName;
                            if("db_full_pull_requests" != tName
                                && "db_heartbeat_monitor" != tName
                                && "DB_HEARTBEAT_MONITOR" != tName
                                && "META_SYNC_EVENT" != tName
                                && "DB_FULL_PULL_REQUESTS" != tName
                            ) {
                                tableParams.push({
                                    dsID: param.dsId,
                                    schemaID: schemaId1,
                                    schemaName: param.schemaName,
                                    tableName: tables[e].tableName,
                                    physicalTableRegex: tables[e].physicalTableRegex,
                                    outputTopic: tables[e].outputTopic,
                                    status: "abort"
                                });
                            }
                        }

                        var dsType = param.dsType;
                        if("mysql" == dsType) {
                            var outputTopic = param.dsName + ".dbus" + ".result";
                            tableParams.push({
                                dsID: param.dsId,
                                schemaID: schemaId2,
                                schemaName: "dbus",
                                tableName: "db_heartbeat_monitor",
                                physicalTableRegex: "db_heartbeat_monitor",
                                outputTopic: outputTopic,
                                status: "abort"
                            });
                            tableParams.push({
                                dsID: param.dsId,
                                schemaID: schemaId2,
                                schemaName: "dbus",
                                tableName: "db_full_pull_requests",
                                physicalTableRegex: "db_full_pull_requests",
                                outputTopic: outputTopic,
                                status: "abort"
                            });
                        } else {
                            var outputTopic = param.dsName + ".DBUS" + ".result";
                            tableParams.push({
                                dsID: param.dsId,
                                schemaID: schemaId2,
                                schemaName: "DBUS",
                                tableName: "META_SYNC_EVENT",
                                physicalTableRegex: "META_SYNC_EVENT",
                                outputTopic: outputTopic,
                                status: "abort"
                            });
                            tableParams.push({
                                dsID: param.dsId,
                                schemaID: schemaId2,
                                schemaName: "DBUS",
                                tableName: "DB_HEARTBEAT_MONITOR",
                                physicalTableRegex: "DB_HEARTBEAT_MONITOR",
                                outputTopic: outputTopic,
                                status: "abort"
                            });
                            tableParams.push({
                                dsID: param.dsId,
                                schemaID: schemaId2,
                                schemaName: "DBUS",
                                tableName: "DB_FULL_PULL_REQUESTS",
                                physicalTableRegex: "DB_FULL_PULL_REQUESTS",
                                outputTopic: outputTopic,
                                status: "abort"
                            });
                        }
                
                        //列出已经插入到管理库中的所有表信息
                        TableService.listAllManagerTables( function searchManagerTables(err,response){
                            if (err) {
                                res.json({status: 500, message: err.message});
                                return;
                            }
                            //保存了管理库中所有的表信息
                            var tableList = JSON.parse(response.body);
                            //由于schemaId和tableName构成了dataTable的唯一性索引，所以可根据这两个参数判定是否table信息已经录入。
                            var tablesInfo = [];
                            for(var index in tableList)
                            {
                                tablesInfo.push({schemaId:tableList[index].schemaID,tableName:tableList[index].tableName});
                            }
                
                            var len = tableParams.length - 1;
                            for (var e in tableParams) {
                                var schemaId = tableParams[e].schemaID;
                                var tableName = tableParams[e].tableName;
                                var flag = true;
                                for(var j in tableList)
                                {
                                    //如果参数一致，表示table信息已经录入，将flag置为false。
                                    if(schemaId == tableList[j].schemaID && tableName == tableList[j].tableName)
                                    {
                                        flag = false;
                                    }
                                }
                                //flag为true，表示该table信息还未录入，将table信息插入到管理库中。
                                if(flag)
                                {
                                    service.insertTable(tableParams[e], function insertTable(err,response) {
                                        if (err) {
                                            res.json({status: 500, message: err.message});
                                            return;
                                        }
                                    });
                                }
                                if(e == len)
                                {
                                    res.json({status: 200, data: "OK"});
                                    return;
                                }
                            }
                        });
                    } else {
                        //表信息，保存了所有需要插入的表信息，如果非空，则执行以下逻辑。
                        var tableParams = [];
                        var dsType = param.dsType;
                        if("mysql" == dsType) {
                            var outputTopic = param.dsName + ".dbus" + ".result";
                            tableParams.push({
                                dsID: param.dsId,
                                schemaID: schemaId2,
                                schemaName: "dbus",
                                tableName: "db_heartbeat_monitor",
                                physicalTableRegex: "db_heartbeat_monitor",
                                outputTopic: outputTopic,
                                status: "abort"
                            });
                            tableParams.push({
                                dsID: param.dsId,
                                schemaID: schemaId2,
                                schemaName: "dbus",
                                tableName: "db_full_pull_requests",
                                physicalTableRegex: "db_full_pull_requests",
                                outputTopic: outputTopic,
                                status: "abort"
                            });
                        } else {
                            var outputTopic = param.dsName + ".DBUS" + ".result";
                            tableParams.push({
                                dsID: param.dsId,
                                schemaID: schemaId2,
                                schemaName: "DBUS",
                                tableName: "META_SYNC_EVENT",
                                physicalTableRegex: "META_SYNC_EVENT",
                                outputTopic: outputTopic,
                                status: "abort"
                            });
                            tableParams.push({
                                dsID: param.dsId,
                                schemaID: schemaId2,
                                schemaName: "DBUS",
                                tableName: "DB_HEARTBEAT_MONITOR",
                                physicalTableRegex: "DB_HEARTBEAT_MONITOR",
                                outputTopic: outputTopic,
                                status: "abort"
                            });
                            tableParams.push({
                                dsID: param.dsId,
                                schemaID: schemaId2,
                                schemaName: "DBUS",
                                tableName: "DB_FULL_PULL_REQUESTS",
                                physicalTableRegex: "DB_FULL_PULL_REQUESTS",
                                outputTopic: outputTopic,
                                status: "abort"
                            });
                        }
                
                        //列出已经插入到管理库中的所有表信息
                        TableService.listAllManagerTables( function searchManagerTables(err,response){
                            if (err) {
                                res.json({status: 500, message: err.message});
                                return;
                            }
                            //保存了管理库中所有的表信息
                            var tableList = JSON.parse(response.body);
                            //由于schemaId和tableName构成了dataTable的唯一性索引，所以可根据这两个参数判定是否table信息已经录入。
                            var tablesInfo = [];
                            for(var index in tableList)
                            {
                                tablesInfo.push({schemaId:tableList[index].schemaID,tableName:tableList[index].tableName});
                            }
                
                            var len = tableParams.length - 1;
                            for (var e in tableParams) {
                                var schemaId = tableParams[e].schemaID;
                                var tableName = tableParams[e].tableName;
                                var flag = true;
                                for(var j in tableList)
                                {
                                    //如果参数一致，表示table信息已经录入，将flag置为false。
                                    if(schemaId == tableList[j].schemaID && tableName == tableList[j].tableName)
                                    {
                                        flag = false;
                                    }
                                }
                                //flag为true，表示该table信息还未录入，将table信息插入到管理库中。
                                if(flag)
                                {
                                    service.insertTable(tableParams[e], function insertTable(err,response) {
                                        if (err) {
                                            res.json({status: 500, message: err.message});
                                            return;
                                        }
                                    });
                                }
                                if(e == len)
                                {
                                    res.json({status: 200, data: "OK"});
                                    return;
                                }
                            }
                        });
                
                    }
                } catch (error) {
                    logger.error("Error occurred while process tableField :%j.\n Error message -- %s \n%s", dataList, error.message, error.stack);
                    res.json({status: -1, error: error.message});
                }
            });
        } else {
            //如果两个schema都已经插入到管理库中，则根据两个schema的id，插入表信息
            //表信息，保存了所有需要插入的表信息，如果非空，则执行以下逻辑。
            if(typeof (tables) != "undefined") {
                var tableParams = [];
                for (var e in tables) {
                    tableParams.push({
                        dsID: param.dsId,
                        schemaID: schemaId1,
                        schemaName: param.schemaName,
                        tableName: tables[e].tableName,
                        physicalTableRegex: tables[e].physicalTableRegex,
                        outputTopic: tables[e].outputTopic,
                        status: "abort"
                    });
                }

                var dsType = param.dsType;
                if("mysql" == dsType) {
                    var outputTopic = param.dsName + ".dbus" + ".result";
                    tableParams.push({
                        dsID: param.dsId,
                        schemaID: schemaId2,
                        schemaName: "dbus",
                        tableName: "db_heartbeat_monitor",
                        physicalTableRegex: "db_heartbeat_monitor",
                        outputTopic: outputTopic,
                        status: "abort"
                    });
                    tableParams.push({
                        dsID: param.dsId,
                        schemaID: schemaId2,
                        schemaName: "dbus",
                        tableName: "db_full_pull_requests",
                        physicalTableRegex: "db_full_pull_requests",
                        outputTopic: outputTopic,
                        status: "abort"
                    });
                } else {
                    var outputTopic = param.dsName + ".DBUS" + ".result";
                    tableParams.push({
                        dsID: param.dsId,
                        schemaID: schemaId2,
                        schemaName: "DBUS",
                        tableName: "META_SYNC_EVENT",
                        physicalTableRegex: "META_SYNC_EVENT",
                        outputTopic: outputTopic,
                        status: "abort"
                    });
                    tableParams.push({
                        dsID: param.dsId,
                        schemaID: schemaId2,
                        schemaName: "DBUS",
                        tableName: "DB_HEARTBEAT_MONITOR",
                        physicalTableRegex: "DB_HEARTBEAT_MONITOR",
                        outputTopic: outputTopic,
                        status: "abort"
                    });
                    tableParams.push({
                        dsID: param.dsId,
                        schemaID: schemaId2,
                        schemaName: "DBUS",
                        tableName: "DB_FULL_PULL_REQUESTS",
                        physicalTableRegex: "DB_FULL_PULL_REQUESTS",
                        outputTopic: outputTopic,
                        status: "abort"
                    });
                }

                //列出已经插入到管理库中的所有表信息
                TableService.listAllManagerTables( function searchManagerTables(err, response) {
                    if (err) {
                        res.json({status: 500, message: err.message});
                        return;
                    }
                    //保存了管理库中所有的表信息
                    var tableList = JSON.parse(response.body);
                    //由于schemaId和tableName构成了dataTable的唯一性索引，所以可根据这两个参数判定是否table信息已经录入。
                    var tablesInfo = [];
                    for(var index in tableList)
                    {
                        tablesInfo.push({schemaId:tableList[index].schemaID, tableName:tableList[index].tableName});
                    }

                    var len = tableParams.length - 1;
                    for (var e in tableParams) {
                        var schemaId = tableParams[e].schemaID;
                        var tableName = tableParams[e].tableName;
                        var flag = true;
                        for(var j in tableList)
                        {
                            //如果参数一致，表示table信息已经录入，将flag置为false。
                            if(schemaId == tableList[j].schemaID && tableName == tableList[j].tableName)
                            {
                                flag = false;
                            }
                        }
                        //flag为true，表示该table信息还未录入，将table信息插入到管理库中。
                        if(flag)
                        {
                            service.insertTable(tableParams[e], function insertTable(err,response) {
                                if (err) {
                                    res.json({status: 500, message: err.message});
                                    return;
                                }
                            });
                        }
                        if(e == len)
                        {
                            res.json({status: 200, data: "OK"});
                            return;
                        }
                    }
                });
            } else {
                //表信息，保存了所有需要插入的表信息，如果非空，则执行以下逻辑。
                var tableParams = [];
                var dsType = param.dsType;
                if("mysql" == dsType) {
                    var outputTopic = param.dsName + ".dbus" + ".result";
                    tableParams.push({
                        dsID: param.dsId,
                        schemaID: schemaId2,
                        schemaName: "dbus",
                        tableName: "db_heartbeat_monitor",
                        physicalTableRegex: "db_heartbeat_monitor",
                        outputTopic: outputTopic,
                        status: "abort"
                    });
                    tableParams.push({
                        dsID: param.dsId,
                        schemaID: schemaId2,
                        schemaName: "dbus",
                        tableName: "db_full_pull_requests",
                        physicalTableRegex: "db_full_pull_requests",
                        outputTopic: outputTopic,
                        status: "abort"
                    });
                } else {
                    var outputTopic = param.dsName + ".DBUS" + ".result";
                    tableParams.push({
                        dsID: param.dsId,
                        schemaID: schemaId2,
                        schemaName: "DBUS",
                        tableName: "META_SYNC_EVENT",
                        physicalTableRegex: "META_SYNC_EVENT",
                        outputTopic: outputTopic,
                        status: "abort"
                    });
                    tableParams.push({
                        dsID: param.dsId,
                        schemaID: schemaId2,
                        schemaName: "DBUS",
                        tableName: "DB_HEARTBEAT_MONITOR",
                        physicalTableRegex: "DB_HEARTBEAT_MONITOR",
                        outputTopic: outputTopic,
                        status: "abort"
                    });
                    tableParams.push({
                        dsID: param.dsId,
                        schemaID: schemaId2,
                        schemaName: "DBUS",
                        tableName: "DB_FULL_PULL_REQUESTS",
                        physicalTableRegex: "DB_FULL_PULL_REQUESTS",
                        outputTopic: outputTopic,
                        status: "abort"
                    });
                }

                //列出已经插入到管理库中的所有表信息
                TableService.listAllManagerTables( function searchManagerTables(err, response) {
                    if (err) {
                        res.json({status: 500, message: err.message});
                        return;
                    }
                    //保存了管理库中所有的表信息
                    var tableList = JSON.parse(response.body);
                    //由于schemaId和tableName构成了dataTable的唯一性索引，所以可根据这两个参数判定是否table信息已经录入。
                    var tablesInfo = [];
                    for(var index in tableList)
                    {
                        tablesInfo.push({schemaId:tableList[index].schemaID,tableName:tableList[index].tableName});
                    }

                    var len = tableParams.length - 1;
                    for (var e in tableParams) {
                        var schemaId = tableParams[e].schemaID;
                        var tableName = tableParams[e].tableName;
                        var flag = true;
                        for(var j in tableList)
                        {
                            //如果参数一致，表示table信息已经录入，将flag置为false。
                            if(schemaId == tableList[j].schemaID && tableName == tableList[j].tableName)
                            {
                                flag = false;
                            }
                        }
                        //flag为true，表示该table信息还未录入，将table信息插入到管理库中。
                        if(flag)
                        {
                            service.insertTable(tableParams[e], function insertTable(err,response) {
                                if (err) {
                                    res.json({status: 500, message: err.message});
                                    return;
                                }
                            });
                        }
                        if(e == len)
                        {
                            res.json({status: 200, data: "OK"});
                            return;
                        }
                    }
                });
            }
        }
    }).catch(function (e) {
        res.json({status: -1, error: e.error.message || e.response.body});
    });
});

var helper = {
    listSchemaId: function (param) {
        return new Promise(function (resolve, reject) {
            //首先根据schemaName查询出管理库dataSchema表中所有的schema(复用后台的根据schema名字查询schema列表接口)，
            // 再从获取的schema列表中获取dsId和schemaName（dsId和schemaName构成唯一性索引），然后与前端传入的参数进行比较，
            //如果两个参数完全一致，则表示传入的schema信息先前已经插入到管理库中，否则将schema信息插入到管理库中。
            SchemaService.listManagerSchemaByDsId(param.dsId, function searchManagerSchema(err,response) {
                if (err) {
                    //response.json({status: 500, message: err.message});
                    return;
                }
                var schemaList = JSON.parse(response.body);
                var name = param["schemaName"];
                var dsId = param["dsId"];
                var dsType = param["dsType"];
                var schemaFlag1 = false;
                var schemaFlag2 = false;
                var schemaId1 = -1;
                var schemaId2 = -1;

                //根据dsId查询出schema列表，判断要插入的schema及dbus schema是否已经在管理库中，
                //如果都存在，则将两个schema的id及标志位返回，供插入相关表使用；如果某个schema不存在，
                // 则返回其schema的id及标志位（为了统一处理），由于该schema的标志位为false，则需要将该schema插入到管理库中
                for(var i in schemaList)
                {
                    //两个参数一致，表示schema信息已经插入到管理库中，此时需要获取该schema信息的id,
                    //根据该schema信息的id将table信息录入到管理库中。
                    if(dsId == schemaList[i].dsId && name == schemaList[i].schemaName)
                    {
                        schemaFlag1 = true;
                        schemaId1 = schemaList[i].id;
                    }
                    if("mysql" == dsType) {
                        if(dsId == schemaList[i].dsId && "dbus" == schemaList[i].schemaName) {
                            schemaFlag2 = true;
                            schemaId2 = schemaList[i].id;
                        }
                    } else {
                        if(dsId == schemaList[i].dsId && "DBUS" == schemaList[i].schemaName) {
                            schemaFlag2 = true;
                            schemaId2 = schemaList[i].id;
                        }
                    }
                }
                resolve({schemaFlag1:schemaFlag1, schemaId1:schemaId1, schemaFlag2:schemaFlag2, schemaId2:schemaId2});
            });
        });
    }
}

module.exports = router;
