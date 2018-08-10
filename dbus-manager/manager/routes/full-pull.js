var express = require('express');
var router = express.Router();
var $ = require('../lib/utils/utils');
var df = require('dateformat');
var dsService = require('../lib/service/ds-service');
var fpService = require('../lib/service/fullPull-service');
var ZooKeeper = require ('node-zookeeper-client');
var config = require('../config');
var client = ZooKeeper.createClient(config.zk.connect);
var now = function () {
    return (new Date()).getTime();
};
var format = function (date) {
    df(date, "yyyy-MM-dd HH:mm:ss.SSS");
};

//#message.type=FULL_DATA_PULL_REQ,
//#message.type=APPENDER_TOPIC_RESUME, // appender 唤醒暂停的consumer
//#message.type=APPENDER_RELOAD_CONFIG, // appender 重新加载配置
//#message.type=MONITOR_ALARM, // 监控报警, appender用来停止伪心跳

var buildTemplate = function (from, type, payload) {
    var date = new Date();
    return {
        from: from,
        payload: payload || {},
        type: type
    };
};

router.get('/', function (req, res) {
    var dsId = req.query.dsId;
    var schemaName = req.query.schemaName;
    var tableName = req.query.tableName;
    var resultTopic = req.query.resultTopic;
    var version = req.query.version;
    var batch = req.query.batch;
    var messageType = req.query.messageType;
    var physicalTables = req.query.physicalTables;
    var payload = {
        "SCHEMA_NAME": schemaName, //【变动部分】用户指定的schema的名称
        "TABLE_NAME": tableName,//【变动部分】用户指定的表的名称
        "DBUS_DATASOURCE_ID": dsId,//【变动部分】用户指定的datasource的id
        "INCREASE_VERSION":version, //【变动部分】用户指定。
        "INCREASE_BATCH_NO":batch, //【变动部分】用户指定。
        "resultTopic":resultTopic,//【变动部分】自动按一定规则生成，用户可修改指定
        "OP_TS":null,//【必须提供】从kafka获取。当前appender增量数据最后一条的ts
        "POS":null, //【必须提供】从kafka获取。当前appender增量数据最后一条的POS
        "SEQNO":"1", //【不变部分】【必须提供】字符串内容可指定为任意整数，不可为非整数
        "PHYSICAL_TABLES": physicalTables, //【必须提供】

//---------------以下为【可不提供】非关键字段------
        "PULL_REMARK": "",//【可不提供】
        "SPLIT_BOUNDING_QUERY": "",//【可不提供】
        "PULL_TARGET_COLS": "", //【可不提供】
        "SCN_NO": "",//【可不提供】
        "SPLIT_COL": ""//【可不提供】
    };
    var typeList = [];
    var type = "FULL_DATA_INDEPENDENT_PULL_REQ";
    if(messageType=='单表独立全量拉取') {
        typeList.push({
            type: type,
            text: messageType,
            template: buildTemplate('independent-pull-request-from-CtrlMsgSender',type,payload)
        });
    } else if(messageType=='全局独立全量拉取') {
        typeList.push({
            type: type,
            text: messageType,
            template: buildTemplate('global-pull-request-from-CtrlMsgSender',type,payload)
        });
    }

    res.json({status: 200, data: typeList});
});

router.post('/send', function (req, res) {
    fpService.send(req.body, function (e, response) {
        if (e || response.body.status != 200) {
            res.json({status: 500, message: "发送 独立拉全量请求 失败"});
            return;
        }
        res.json({status: 200, message: "ok"});
    });
});

module.exports = router;

