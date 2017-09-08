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

router.get('/listTable', function (req, res) {
    var param = helper.buildParam(req.query, ["dsID","dsName", "schemaName"]);
    // if (!param.pageSize) {
    //     param.pageSize = 10;
    // }
    // if (!param.pageNum) {
    //     param.pageNum = 1;
    // }

    helper.listTables(param).then(function (data) {
        var tablesName = data.tablesName;
        var methods = [];
        var schemaNameParam = [];
        schemaNameParam.push({dsID:param["dsID"], schemaName:param["schemaName"]});
        //先从管理库中查询出指定dsId和schemaName的所有表
        methods.push(tableService.listManagerTables(schemaNameParam[0]));

        for (var key in tablesName) {
            methods.push(service.listTableField(tablesName[key]));
        }

        Promise.all(methods).then(function (dataList) {
            try {
                var managerTables = JSON.parse(dataList[0]);
                var dataListLength = dataList.length;
                var tablesField = [];
                var incompatibleColumn = '';
                var columnName = '';

                for(var i = 1; i < dataListLength; i++)
                {
                    var length = JSON.parse(dataList[i]).length;
                    var name = tablesName[i-1].tableName;
                    var __ckbox_checked__ = false;
                    var __disabled__ = false;
                    var physicalTableRegex = '';
                    var outputTopic = '';
                    for( var j = 0; j < managerTables.length; j++)
                    {
                        if(name == managerTables[j].tableName)
                        {
                            __ckbox_checked__ = true;
                            __disabled__ = true;
                            physicalTableRegex = managerTables[j].physicalTableRegex;
                            outputTopic = managerTables[j].outputTopic;
                            break;
                        }
                    }
                    if(length > 0)
                    {
                        var data = JSON.parse(dataList[i]);
                        columnName = '';
                        incompatibleColumn = '';
                        for(var index in data)
                        {
                            columnName += data[index].columnName + '/' + data[index].dataType + ' ';
                            if(data[index].incompatibleColumn)
                            {
                                incompatibleColumn += data[index].incompatibleColumn + ' ';
                            }
                            else {
                                incompatibleColumn =  '无 ';
                            }
                        }
                        tablesField.push({tableName:tablesName[i-1].tableName,physicalTableRegex:physicalTableRegex,
                            outputTopic:outputTopic,columnName:columnName,incompatibleColumn:incompatibleColumn,
                            __ckbox_checked__:__ckbox_checked__, __disabled__:__disabled__});
                    }
                    else {
                        tablesField.push({tableName:tablesName[i-1].tableName,
                            physicalTableRegex:physicalTableRegex,outputTopic:outputTopic,
                            columnName:'无',incompatibleColumn:'无',__ckbox_checked__:__ckbox_checked__, __disabled__:__disabled__});
                    }
                }
                res.json({status: 200,data: tablesField});

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
