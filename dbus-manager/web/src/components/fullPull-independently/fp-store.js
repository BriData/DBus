var React = require('react');
var Reflux = require('reflux');
var $ = require('jquery')
var utils = require('../common/utils');

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

var initSchema = function(result) {
    if (result.status !== 200) {
        alert("加载Schema失败");
        return;
    }
    var list = [];
    result.data.forEach(function(e) {
        /*
        list.push({
            value: e.id,
            text: e.schemaName
        });
        */
        list.push(<option key={e.id} value={e.id} >{e.schemaName}</option>);
    });
    return list;
};

var initSchemaList = function(result) {
    if (result.status !== 200) {
        alert("加载Schema失败");
        return;
    }
    var list = [];
    result.data.forEach(function(e) {

        list.push({
            value: e.id,
            text: e.schemaName
        });

    });
    return list;
};

var initTableList = function(result) {
    if (result.status !== 200) {
        alert("加载table失败");
        return;
    }
    var list = [];
    result.data.forEach(function(e) {
        list.push({
            value: e.tableName,
            text: e.tableName,
            outputTopic:e.outputTopic,
            version:e.version
        });
    });
    return list;
};

var initTable = function(result) {
    if (result.status !== 200) {
        alert("加载table失败");
        return;
    }
    var list = [];
    result.data.forEach(function(e) {
        list.push(<option key={e.tableName} value={e.tableName} >{e.tableName}</option>);
    });
    return list;
};

var actions = Reflux.createActions(['initialLoad', 'sendMessage','createSchemaList','createTableList','createTypeList','setVersion','setBatch','setResultTopic']);
var store = Reflux.createStore({
    //监听所有的actions
    listenables: [actions],
    state: {
        dsOptions: [],
        schemaOptions: [],
        schemaList:[],
        tableOptions: [],
        tableList:[],
        editor: {
            json: {}
        }
    },
    initState: function() {
        return this.state;
    },
    onInitialLoad: function() {
        var self = this;
        $.get(utils.builPath("ds/list"),function(result) {
            self.state.dsOptions = initDatasource(result);
            self.trigger(self.state);
        });
    },
    onCreateSchemaList:function(dsParam){
        var self = this;
       $.get(utils.builPath("schema/list"), dsParam ,function(result){
            self.state.schemaOptions = initSchema(result);
            self.state.schemaList = initSchemaList(result);
            self.trigger(self.state);
       });

    },
    onCreateTableList:function(schemaParam){
       var self = this;
       $.get(utils.builPath("tables/list"), schemaParam ,function(result){
            self.state.tableOptions = initTable(result);
            self.state.tableList = initTableList(result);
            self.trigger(self.state);
       });

    },
    onCreateTypeList:function(typeParam){
       var self = this;
       $.get(utils.builPath("fullPull"), {dsId:typeParam.dsId,schemaName:typeParam.schemaName,tableName:typeParam.tableName,resultTopic:typeParam.resultTopic,version:typeParam.version,batch:typeParam.batch} ,function(result){
           var t = result.data.find(function(t) {
            if (t.text === typeParam.messageType) {
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
        });
    },
    onSetVersion:function(version){
       var self = this;
       if(self.state.editor.json.payload){
        self.state.editor.json.payload.INCREASE_VERSION = version;
        self.trigger(self.state);
       }
    },
    onSetBatch:function(batch){
       var self = this;
       if(self.state.editor.json.payload){
        self.state.editor.json.payload.INCREASE_BATCH_NO = batch;
        self.trigger(self.state);
       }
    },
    onSetResultTopic:function(resultTopic){
       var self = this;
       if(self.state.editor.json.payload){
        self.state.editor.json.payload.resultTopic = resultTopic;
        self.trigger(self.state);
       }
    },
    onSendMessage: function(ctrlTopic,outputTopic,message,callback) {
        var strJson = JSON.stringify(message);
          $.ajax({
            url:utils.builPath('fullPull/send'),
            method: "POST",
            data: {ctrlTopic:ctrlTopic,outputTopic:outputTopic,message:strJson},
            //contentType:"application/json; charset=utf-8",
            dataType:"json"
         }).done(function(result) {
            if(result.status === 200) {
                callback("消息发送成功！");
            } else {
                callback("消息发送失败！");
            }
         }).fail(function(res) {
            callback("消息发送失败！");
         });
    }
});

store.actions = actions;
module.exports = store;
