var express = require('express');
var router = express.Router();
var $ = require('../lib/utils/utils');
var df = require('dateformat');
var dsService = require('../lib/service/ds-service');
var ctlMsgService = require('../lib/service/ctlmsg-service');
var ZooKeeper = require ('node-zookeeper-client');
var config = require('../config');
var textEncoding = require('text-encoding');
var TextDecoder = textEncoding.TextDecoder;
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

var buildTemplate = function (type, payload) {
    return {
        from: "dbus-web",
        id: now(),
        payload: payload || {},
        timestamp: format(new Date()),
        type: type
    };
};

var buildTemplateReaload = function (type) {
    return {
        from: "dbus-web",
        type: type
    };
};

router.get('/', function (req, res) {
    var typeList = [
        {
            type: "EXTRACTOR_RELOAD_CONF",
            text: "Reload Extractor",
            template: buildTemplate('EXTRACTOR_RELOAD_CONF')
        },
        {
            type: "DISPATCHER_RELOAD_CONFIG",
            text: "Reload Dispatcher",
            template: buildTemplate('DISPATCHER_RELOAD_CONFIG')
        },
        {
            type: "APPENDER_RELOAD_CONFIG",
            text: "Reload Appender",
            template: buildTemplate('APPENDER_RELOAD_CONFIG')
        },
        {
            type: "FULL_DATA_PULL_RELOAD_CONF",
            text: "Reload Splitter-Fullpuller",
            template: buildTemplateReaload('FULL_DATA_PULL_RELOAD_CONF')
        },
        {
            type: "HEARTBEAT_RELOAD_CONFIG",
            text: "Reload Heartbeat",
            template: buildTemplate('HEARTBEAT_RELOAD_CONFIG')
        }
    ];
    res.json({status: 200, data: typeList});
});

router.get('/send', function (req, res) {
    var param = req.query;
    var message = param.message;

    var type = message.type;
    if(type != "HEARTBEAT_RELOAD_CONFIG") {
        var ctrlTopic = param.ctrlTopic;
        ctlMsgService.send(ctrlTopic, JSON.stringify(message), function (e, response) {
            if (e || response.body.status != 0) {
                res.json({status: 500, message: "发送 control message 失败"});
                return;
            }
            res.json({status: 200, message: "ok"});
        });
    }else{
        var client = ZooKeeper.createClient(config.zk.connect,config.zk.client);
        client.once('connected', function () {
            console.log('Connected to the server.');
             var zkParam ={
             cmdType:"1",
             args:message.id
             };
            var data=new Buffer(JSON.stringify(zkParam));
            client.setData("/DBus/HeartBeat/Control", data, function (error, stat) {
                if (error) {
                    console.log(error.stack);
                    res.json({status: 500});
                    return;
                }
                else res.json({status: 200});
                client.close();
                console.log('connection closed.');
            });
        });
        client.connect();
    }
});

router.get("/readZkNode",function(req, res) {
    var param=req.query;
    var messageType = param.messageType;

    helper.getNodeData(messageType,"",res).then(function(data){

        var client = ZooKeeper.createClient(config.zk.connect,config.zk.client);
        client.once('connected', function() {
            console.log('Connected to the server.');
            client.getChildren("/DBus/ControlMessageResult/" + data.path,function (error, children,stat) {
                if (error) {
                    console.log(error.stack);
                    res.json({status: 500});
                    return;
                }
                if(!$.isEmpty(children)){

                    var methods = [];
                    for(var item in children){
                        methods.push(helper.getNodeData(data.path,children[item],res));
                    }
                    Promise.all(methods).then(function(dataList){
                        var childrenData = "";
                        var parentData = data.nodeData;
                        var i;
                        for(i =0;i<dataList.length-1;i++){
                            var path = dataList[i].path.split("/");
                            var tempChildren = dataList[i].nodeData.substr(0,dataList[i].nodeData.length-2);
                            childrenData = childrenData  + path[1] + ": " + tempChildren + "\n\t" + "}," + "\n\t";
                        }
                        var pathLast = dataList[i].path.split("/");
                        var tempChildrenLast = dataList[i].nodeData.substr(0,dataList[i].nodeData.length-2);
                        childrenData = childrenData  + pathLast[1] + ": " + tempChildrenLast + "\n\t" + "}";


                        var tempParent = parentData.substr(0,parentData.length-2);
                        parentData = tempParent + "," + "\n\t" + childrenData +  "\n" + "}";
                        res.json({status: 200,data: parentData});
                    })
                } else {
                    res.json({status: 200,data: data.nodeData});
                }
            });
            client.close();
        });
        client.connect();

    });
});

var helper = {
    getNodeData: function (path,nodeName,res) {
        return new Promise(function (resolve, reject) {

            var client = ZooKeeper.createClient(config.zk.connect,config.zk.client);
             if(nodeName != ""){
                 path = path + "/" + nodeName;
             }
            client.once('connected', function() {
                console.log('Connected to the server. read children data.');

                client.getData("/DBus/ControlMessageResult/" + path, function (event) {
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
                    var nodeData = new TextDecoder("utf-8").decode(data);
                    resolve({nodeData:nodeData,path:path});
                    //var json = JSON.parse(parentData);
                    //res.json({status: 200, data: parentData});
                });
                client.close();
            });
            client.connect();
        });
    }
}
module.exports = router;
