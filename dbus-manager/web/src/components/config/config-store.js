var React = require('react');
var Reflux = require('reflux');
var $ = require('jquery')
var utils = require('../common/utils');

var actions = Reflux.createActions([ 'storm_check','global_config','monitor_config','storm_config','user_config','heartbeatInterval','checkInterval','checkFullPullInterval', 'deleteFullPullOldVersionInterval',
                                     'maxAlarmCnt', 'heartBeatTimeout',"fullPullTimeout",'alarmTtl":"lifeInterval','correcteValue','fullPullCorrecteValue',
                                     'fullPullSliceMaxPending','leaderPath','controlPath','monitorPath','monitorFullPullPaht','excludeSchema',
                                     'checkPointPerHeartBeatCnt','fullPullOldVersionCnt','adminSMSNo','adminUseSMS','adminEmail',
                                     'adminUseEmail','schemaChangeEmail','schemaChangeUseEmail','initialLoad','initialLoadds','initialLoadschema','load','savezk','save_heart_conf','stormRestChange','checkStormAvailable']);
var store = Reflux.createStore({
    state: {
        data: null,
        dsOptions: [],
        schema:[],
        ds_schema_list:[],
        storm_success:'N'
    },
    initState: function() {
        return this.state;
    },
    onInitialLoad:function(){
        var self = this;
        $.get(utils.builPath("config/initialLoad"), function(result) {
            if(result.status !== 200) {
                console.info("加载失败");
                alert("加载失败");
                return;
            }
            self.state.data=result.data;
            if(!self.state.data.heartbeat_config.hasOwnProperty("heartbeatInterval")){
                 alert("心跳信息没有初始化，请进行初始化配置");
            }
            if(!self.state.data.heartbeat_config.hasOwnProperty("additionalNotify")){
                self.state.data.heartbeat_config.additionalNotify = {};
            }
            if(!self.state.data.heartbeat_config.hasOwnProperty("heartBeatTimeoutAdditional")){
                self.state.data.heartbeat_config.heartBeatTimeoutAdditional = {};
            }
            if(!self.state.data.heartbeat_config.hasOwnProperty("schemaChangeEmail")){
                self.state.data.heartbeat_config.schemaChangeEmail = " ";
            }
            if(!self.state.data.heartbeat_config.hasOwnProperty("schemaChangeUseEmail")){
                self.state.data.heartbeat_config.schemaChangeUseEmail = " ";
            }
            self.trigger(self.state);
            utils.hideLoading();
        });
    },
    onInitialLoadds: function() {
        var self = this;
        $.get(utils.builPath("ds/list"),function(result) {
            if(result.status !== 200) {
                console.info("加载失败");
                alert("加载失败");
                return;
            }
            console.info("dsOptionsdsOptionsdsOptionsdsOptionsdsOptions");
            console.info(result.data);
            self.state.dsOptions = result.data;
            self.trigger(self.state);
            utils.hideLoading();
        });
    },
    onInitialLoadschema: function() {
        var self = this;
        $.get(utils.builPath("config/search"), function(result) {
            if(result.status !== 200) {
                if(console)
                    console.error(JSON.stringify(result));
                alert("加载Schema失败");
                return;
            }
            var temp = result.data;
            console.info("schemaschemaschemaschemaschemaschema");
            console.info(result.data);
            var temp1 = JSON.parse(temp);
             console.info("temp1temp1temp1temp1");
            console.info(temp1);
            console.info(temp1.length);
            var schema_list = [];
            for (var i = 0;i < temp1.length;i++){
                schema_list.push(temp1[i].dsName+'/'+temp1[i].schemaName)
            }
            console.info(schema_list);
            self.state.schema = schema_list;
            self.trigger(self.state);
            utils.hideLoading();
        });
    },
    onStorm_check:function(){
        var self = this;
        var param = self.state.data.global_config.storm;
        console.info(typeof(param));
        var index = param.indexOf("/");
        var length = param.length;
        var path_storm = param.substring(index,length);
        console.info("path_storm"+path_storm);
        var self = this;
        var flag = false;
        var paramp = {};
        paramp.path = param;
        $.get(utils.builPath("config/stormcheck"),paramp, function(result) {
            if(result.status !== 200) {
                var data = result.data.toString().replace(/\n/gm, "<br/>");
                alert("ssh免密登录失败,请配置");
                utils.hideLoading();
                return;
            }
            
            var data = result.data.toString().replace(/\n/gm, "<br/>");
            console.log("storm_check" + data);
            var idx = data.indexOf(path_storm);
            console.log("idx: " + idx);

            if(idx != -1) {
                flag = true;
                self.state.storm_success = 'Y';
                self.trigger(self.state);
                alert("ssh免密登录成功");
            } 
            else {
                alert("ssh免密登录失败,请配置");
            }
            self.trigger(self.state);
            utils.hideLoading();
        });
    },
    onGlobal_config:function(new_value){
        var self = this;
        self.state.data.global_config.bootstrap= new_value;
    },
    onMonitor_config:function(new_value){
        var self = this;
        self.state.data.global_config.monitor_url= new_value;
    },
    onStorm_config:function(new_value){
        var self = this;
        self.state.data.global_config.storm= new_value;
    },
    onUser_config:function(new_value){
        var self = this;
        self.state.data.global_config.user= new_value;
    },
    onStormRestChange:function(new_value){
        var self = this;
        self.state.data.global_config.stormRest= new_value;
    },
    onHeartbeatInterval:function(new_value){
        var self = this;
        self.state.data.heartbeat_config.heartbeatInterval = new_value;
    },
    onCheckInterval:function(new_value){
        var self = this;
        self.state.data.heartbeat_config.checkInterval = new_value;
    },
    onCheckFullPullInterval:function(new_value){
        var self = this;
        self.state.data.heartbeat_config.checkFullPullInterval = new_value;
    },
    onDeleteFullPullOldVersionInterval:function(new_value){
        var self = this;
        self.state.data.heartbeat_config.deleteFullPullOldVersionInterval = new_value;
    },
    onMaxAlarmCnt:function(new_value){
        var self = this;
        self.state.data.heartbeat_config.maxAlarmCnt = new_value;
    },
    onHeartBeatTimeout:function(new_value){
        var self = this;
        self.state.data.heartbeat_config.heartBeatTimeout = new_value;
    },
    onFullPullTimeout:function(new_value){
        var self = this;
        self.state.data.heartbeat_config.fullPullTimeout = new_value;
    },
    onAlarmTtl:function(new_value){
        var self = this;
        self.state.data.heartbeat_config.alarmTtl = new_value;
    },
    onLifeInterval:function(new_value){
        var self = this;
        self.state.data.heartbeat_config.lifeInterval = new_value;
    },
    onCorrecteValue:function(new_value){
        var self = this;
        self.state.data.heartbeat_config.correcteValue = new_value;
    },
    onFullPullCorrecteValue:function(new_value){
        var self = this;
        self.state.data.heartbeat_config.fullPullCorrecteValue = new_value;
    },
    onFullPullSliceMaxPending:function(new_value){
        var self = this;
        self.state.data.heartbeat_config.fullPullSliceMaxPending = new_value;
    },
    onExcludeSchema:function(new_value){
        var self = this;
        self.state.data.heartbeat_config.excludeSchema = new_value;
    },
    onCheckPointPerHeartBeatCnt:function(new_value){
        var self = this;
        self.state.data.heartbeat_config.checkPointPerHeartBeatCnt = new_value;
    },
    onFullPullOldVersionCnt:function(new_value){
        var self = this;
        self.state.data.heartbeat_config.fullPullOldVersionCnt = new_value;
    },
    onAdminSMSNo:function(new_value){
        var self = this;
        self.state.data.heartbeat_config.adminSMSNo = new_value;
    },
    onAdminUseSMS:function(value){
        var self = this;
        self.state.data.heartbeat_config.adminUseSMS = value;
    },
    onAdminEmail:function(new_value){
        var self = this;
        self.state.data.heartbeat_config.adminEmail = new_value;
    },
    onAdminUseEmail:function(value){
        var self = this;
        self.state.data.heartbeat_config.adminUseEmail = value;
    },
    onSchemaChangeEmail:function(new_value){
        var self = this;
        self.state.data.heartbeat_config.schemaChangeEmail = new_value;
    },
    onSchemaChangeUseEmail:function(value){
        var self = this;
        self.state.data.heartbeat_config.schemaChangeUseEmail = value;
    },


    //监听所有的actions
    listenables: [actions],

    onCheckStormAvailable: function(p) {
        var self = this;
        var checkStormCount = 2;
        $.ajax({
            type: 'GET',
            url: utils.builPath("jarManager/getJarList"),
            timeout: 10000,
            data: p,
            success: function(result) {
                if(result.status !== 200) {
                    alert("storm启动脚本路径、storm机器登录用户名、免密登陆配置 错误");
                    return;
                }
                checkStormCount--;
                if(checkStormCount == 0) {
                    alert("Storm配置检测正确");
                }
            },
            error: function (xml, errType, e) {
                alert("storm启动脚本路径、storm机器登录用户名、免密登陆配置 错误");
            }
        });
        $.ajax({
            type: 'GET',
            url: utils.builPath("topology/list"),
            timeout: 10000,
            data: p,
            success: function(result) {
                if(result.status !== 200) {
                    alert("Storm UI REST API 配置错误");
                    return;
                }
                checkStormCount--;
                if(checkStormCount == 0) {
                    alert("Storm配置检测正确");
                }
            },
            error: function (xml, errType, e) {
                alert("Storm UI REST API 配置错误");
            }
        });
        alert("正在验证，请稍后");
    },

    onSavezk: function(p) {
        var self = this;
        $.post(utils.builPath("config/savezk"), p,function(result)
        {
            if(result.status == 500) {
                alert("保存全局设置信息失败");
                console.info(result.status );
                return;
            }
            if(result.status == 200) {
                console.info(result.status );
                alert("保存全局设置信息成功");
            }
        }
        );
    },

    onSave_heart_conf:function(p){
        var self = this;

        $.post(utils.builPath("config/save_heart_conf"), {data:JSON.stringify(p)}, function(result)
            {
                if(result.status == 300) {
                    alert("ssh免密登录失败,请配置");
                    console.info(result.status );
                    return;
                }
                if(result.status == 500) {
                    alert("保存心跳设置信息失败");
                    console.info(result.status );
                    return;
                }
                if(result.status == 200) {
                    console.info(result.status );
                    alert("保存心跳设置信息成功");
                }
            }
        );
    }

});

store.actions = actions;
module.exports = store;
