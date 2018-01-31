var Reflux = require('reflux');
var $ = require('jquery')
var utils = require('../../common/utils');

var actions = Reflux.createActions(['initialLoad','handleSubmit','handleNameBlur','handleURL','typeChange']);

var store = Reflux.createStore({
    //监听所有的actions
    listenables: [actions],
    state: {
        style:"",
        data: {},
        isHideJdbcInfo: false
    },
    initState: function() {
        return this.state;
    },
    onInitialLoad: function() {
        //alert("数据源添加页面加载");
    },
    onHandleNameBlur:function(name,handleSubmit){
        var self = this;
        $.get(utils.builPath("ds/checkName"), name, function(result) {
            console.log(result.data);
            if(result.data[0] !== null) {
                utils.hideLoading();
                alert("数据源存在同名");
                self.trigger(self.state);
                return;
            }
            handleSubmit();
            self.trigger(self.state);
        });
    },
    onHandleURL:function(url, handleNameBlur,handleSubmit){
        var self = this;
        $.get(utils.builPath("ds/validate"), url, function(result) {
            if(result.data !== 1) {
                utils.hideLoading();
                alert("URL不可用(请注意用户名、密码以及url是否正确)");
                self.trigger(self.state);
                return;
            }
            handleNameBlur(handleSubmit);
            self.trigger(self.state);
        });
    },
    onHandleSubmit:function(formdata, callback){
        var self = this;
        $.get(utils.builPath("ds/add"), formdata, function(result) {
            if(result.status !== 200) {
                utils.hideLoading();
                alert("添加数据源失败");
                return;
            }
            utils.hideLoading();
            callback(result.id);
        });
    },
    onTypeChange(value) {
        if(value == utils.dsType.logLogstash 
            || value == utils.dsType.logLogstashJson
            || value == utils.dsType.logUms
            || value == utils.dsType.logFlume
            || value == utils.dsType.logFilebeat) {
            this.state.isHideJdbcInfo = true;
        } else {
            this.state.isHideJdbcInfo = false;
        }
        this.trigger(this.state);
    }
});

store.actions = actions;
module.exports = store;
