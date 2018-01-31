var express = require('express');
var router = express.Router();
var service = require('../lib/service/table-meta-service');
var tableService = require('../lib/service/table-service');
var logger = require('../lib/utils/logger');
var util = require('util')
const ejs = require('ejs');
var template="<h4>No config information</h4>";
try {
    template = require('fs').readFileSync(__dirname + '/../views/config-script-generator.ejs', 'utf-8');
} catch(e) {
    logger.error("There are errors while reading config-script-generator.ejs");
}
router.post('/createScript', function(req, res) {

    // 这里的state是前端传来的，此处转为对象
    const state = JSON.parse(req.body['state']);
    const schemaName = req.body['schemaName'];
    const content = ejs.render(template, {
        state: state,
        schemaName: schemaName
    });
    res.json({status: 200,content: content});
});
router.get('/listPlainlogTable', function(req, res) {
    var param = req.query;
    service.listPlainlogTable(param, function(err, response) {
        if(err) {
            res.json({status: 500, message: err.message});
            return;
        }
        res.json({status: 200, data: response.body});
    })
});

router.get('/createPlainlogTable', function(req, res) {
    var param = req.query;
    service.createPlainlogTable(param, function(err, response) {
        if(err) {
            res.json({status: 500, message: err.message});
            return;
        }
        res.json({status: 200, data: response.body});
    })
});

router.get('/createPlainlogSchemaAndTable', function(req, res) {
    var param = req.query;
    service.createPlainlogSchemaAndTable(param, function(err, response) {
        if(err) {
            res.json({status: 500, message: err.message});
            return;
        }
        res.json({status: 200, data: response.body});
    })
});

router.get('/listTable', function (req, res) {
    var param = helper.buildParam(req.query, ["dsID","dsName", "schemaName"]);

    helper.listTables(param).then(function (data) {
        var sourceTables = data.tablesName;
        var methods = [];
        //先从管理库中查询出指定dsId和schemaName的所有表
        methods.push(tableService.listManagerTables({dsID:param["dsID"], schemaName:param["schemaName"]}));

        methods.push(service.listTableField({sourceTables: sourceTables}));

        Promise.all(methods).then(function (dataList) {
            try {
                var managerTables = JSON.parse(dataList[0]);
                var sourceTableMetas = dataList[1];
                var returnTableInfo = [];

                for(var i = 0; i < sourceTables.length; i++)
                {
                    var meta = sourceTableMetas[i];
                    var metaLength = meta.length;
                    var sourceTableName = sourceTables[i].tableName;
                    var __ckbox_checked__ = false;
                    var __disabled__ = false;
                    var ignoreColumnName = '';
                    var incompatibleColumn = '';
                    var physicalTableRegex = '';
                    var outputTopic = '';
                    for( var j = 0; j < managerTables.length; j++)
                    {
                        if(sourceTableName == managerTables[j].tableName)
                        {
                            __ckbox_checked__ = true;
                            __disabled__ = true;
                            physicalTableRegex = managerTables[j].physicalTableRegex;
                            outputTopic = managerTables[j].outputTopic;
                            break;
                        }
                    }
                    if(metaLength > 0)
                    {
                        for(var index in meta)
                        {
                            ignoreColumnName += meta[index].columnName + '/' + meta[index].dataType + ' ';
                            if(meta[index].incompatibleColumn)
                            {
                                incompatibleColumn += meta[index].incompatibleColumn + ' ';
                            }
                        }
                        if (incompatibleColumn == '') incompatibleColumn = '无';
                        returnTableInfo.push({tableName:sourceTables[i].tableName,physicalTableRegex:physicalTableRegex,
                            outputTopic:outputTopic,columnName:ignoreColumnName,incompatibleColumn:incompatibleColumn,
                            __ckbox_checked__:__ckbox_checked__, __disabled__:__disabled__});
                    }
                    else {
                        returnTableInfo.push({tableName:sourceTables[i].tableName,
                            physicalTableRegex:physicalTableRegex,outputTopic:outputTopic,
                            columnName:'无',incompatibleColumn:'无',__ckbox_checked__:__ckbox_checked__, __disabled__:__disabled__});
                    }
                }
                res.json({status: 200,data: returnTableInfo});

            } catch (error) {
                logger.error("Error occurred while process tableField :%j.\n Error message -- %s \n%s", dataList, error.message, error.stack);
                res.json({status: -1, error: error.message});
            }
        });
    }).catch(function (e) {
        res.json({status: -1, error: err.error.message || err.response.body});
    });
});

var helper = {
    buildParam: function (query, params) {
        var param = {};
        params.forEach(function (key) {
            if (query[key]) {
                param[key] = query[key];
            }
        });
        return param;
    },
    //列出源库中指定dsID,dsName, schemaName的所有table
    listTables: function (param) {
        return new Promise(function (resolve, reject) {
            service.search(param).then(function (data) {
                var tables = JSON.parse(data);
                var tablesName = [];
                tables.forEach(function (t) {
                    tablesName.push({dsName:param["dsName"],schemaName:param["schemaName"],tableName:t.tableName});
                });
                resolve({tablesName: tablesName});
            }).catch(function (e) {
                reject(e);
            });
        });
    }
}

module.exports = router;
