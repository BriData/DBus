var Reflux = require('reflux');
var $ = require('jquery')
var utils = require('../common/utils');

var ReactDOM = require('react-dom');

var initDatasource = function(result) {
    if (result.status !== 200) {
        alert("加载数据源失败");
        return;
    }
    var list = [];
    result.data.forEach(function(e) {
        list.push({
            value: e.id,
            text: [e.dsType, e.dsName].join("/")
        });
    });
    return list;
};
var initMessageType = function(result) {
    if (result.status !== 200) {
        alert("加载数据源失败");
        return;
    }
    return result.data
};

var actions = Reflux.createActions(['initialLoad', 'messageTypeChanged', 'datasourceChanged','sendMessage','readZkNode','closeDialogZK']);
var store = Reflux.createStore({
    //监听所有的actions
    listenables: [actions],
    state: {
        dsOptions: [],
        msgTypeOpts: [],
        editor: {
            json: {}
        },
        dialogCtrl: {
            showJson:false,
            contentJson:"",
            identityJson:"",
        }
    },
    initState: function() {
        return this.state;
    },
    onInitialLoad: function() {
        var self = this;
        $.when($.get(utils.builPath("ds/list")), $.get(utils.builPath("ctlMessage"))).then(function(res1, res2) {
            self.state.dsOptions = initDatasource(res1[0]);
            self.state.msgTypeOpts = initMessageType(res2[0]);
            self.trigger(self.state);
        }, function error(e) {
            alert(e.message);
        });
    },
    onMessageTypeChanged: function(messageType) {
        var self = this;
        var t = self.state.msgTypeOpts.find(function(t) {
            if (t.type === messageType) {
                return t.template;
            }
        });
        if(t == null) return;
        var date = new Date();
        self.state.editor.json = utils.extends(t.template, {
            id: date.getTime(),
            timestamp: date.format('yyyy-MM-dd hh:mm:ss.S')
        });
        self.trigger(self.state);
    },
    onReadZkNode:function(messageType,obj) {
        var self = this;
        if(messageType == "HEARTBEAT_RELOAD_CONFIG"){
           messageType = "HEARTBEAT_RELOAD";
        }
        self.state.dialogCtrl.identityJson = "/DBus/ControlMessageResult/" + messageType;
        self.state.dialogCtrl.showJson = true;
        self.trigger(self.state);
        $.get(utils.builPath("ctlMessage/readZkNode"),{messageType:messageType}).then(function(result) {
            if(result.status !== 200) {
                alert("读取zk节点失败");
                return;
            }     
            
            ReactDOM.findDOMNode(obj.refs.dataResult).value = result.data;
        }, function error(e) {
            alert(e.message);
        });
    },
    onCloseDialogZK:function(){
        this.state.dialogCtrl.showJson = false;
        this.trigger(this.state);
    },
    onDatasourceChanged: function(message) {
        this.state.editor.json = message;
        console.log("this.state.editor.json = message: " + JSON.stringify(message));
        this.trigger(this.state);
    },
    onSendMessage: function(ds,ctrlTopic,message, messageType,callback) {
        var self = this;
        console.log("onSendMessage: " + JSON.stringify(message));
        var param = {
            ds: ds,
            ctrlTopic: ctrlTopic,
            message: message
        }
        $.get(utils.builPath('ctlMessage/send'), param, function(result) {
            if(result.status === 200) {
                callback(message.type + " 消息发送成功！");
            } else {
                callback(message.type + " 消息发送失败！");
            }
        });
    }
});

store.actions = actions;
module.exports = store;
