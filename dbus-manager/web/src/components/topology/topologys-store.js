var Reflux = require('reflux');
var $ = require('jquery')
var utils = require('../common/utils');

var actions = Reflux.createActions(['initialLoad','search','jarChanged','handleSubmit','jarList','selectJar']);

var store = Reflux.createStore({
    //监听所有的actions
    listenables: [actions],
    state: {
        dsOptions: [],
        data: [],
        jarOptions:[],
        dsId:"",
        jarName:""
    },
    initState: function() {
        return this.state;
    },
    onInitialLoad: function() {
        var self = this;
        $.get(utils.builPath("ds/list"), {}, function(result) {
            if(result.status !== 200) {
                alert("加载数据源失败");
                return;
            }
            var list = [];
            result.data.forEach(function(e) {
                list.push({value:e.id, text: [e.dsType, e.dsName].join("/")});
            });
            self.state.dsOptions = list;
            self.onSearch({});
        });

    },

    onSearch: function(p){
        var self = this;
        $.get(utils.builPath("topology/list"), p, function(result) {
            if(result.status !== 200) {
                if(console)
                    console.error(JSON.stringify(result));
                alert("加载Stom Topology失败");
                utils.hideLoading();
                return;
            }
            self.state.data = result.data;

            self.trigger(self.state);
            utils.hideLoading();
        });
    },
    onJarChanged: function(data, selected) {
        data.selectedJar = selected;
        this.trigger(this.state);
    },
    onHandleSubmit:function(formdata,callback){
        var self = this;
        $.get(utils.builPath("topology/addTopology"), formdata, function(result) {
            if(result.status !== 200) {
                alert("添加Topology失败");
                return;
            }
            self.trigger(self.state);
            callback();
        }); 
    },
    onJarList:function(ds){
        var self = this;
        $.get(utils.builPath("topology/selectList"), ds, function(result) {
            if(result.status !== 200) {
                if(console)
                    console.error(JSON.stringify(result));
                alert("加载jar包失败");
                return;
            }
            var list = [];
            result.data.forEach(function(e){
               list.push({value:e, text:e});
            });
            self.state.jarOptions = list;
            self.state.dsId = ds.dsId;
            self.trigger(self.state);
        });
    },
    onSelectJar:function(jar){
        var self = this;
        self.state.jarName = jar.jarName;
        self.trigger(self.state);
    }
});

store.actions = actions;
module.exports = store;
