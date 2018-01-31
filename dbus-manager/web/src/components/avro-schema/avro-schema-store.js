var Reflux = require('reflux');
var $ = require('jquery')
var utils = require('../common/utils');

var actions = Reflux.createActions(['initialLoad','dataSourceSelected','search','closeDialog', 'openDialogByKey']);

var store = Reflux.createStore({
    state: {
        dsOptions: [],
        schemaOpts: [],
        data: [],
        dialog: {
            show: false,
            content:"",
            identity:""
        }
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
            self.trigger(self.state);
        });
    },
    //监听所有的actions
    listenables: [actions],
    onSearch: function(p){
        var self = this;
        $.get(utils.builPath("avroSchema/search"), p, function(result) {
            if(result.status !== 200) {
                if(console)
                    console.error(JSON.stringify(result));
                alert("加载Avro Schema失败");
                return;
            }
            if(console.log("onsearch method called"));
            self.state.data = result.data;
            self.trigger(self.state);
        });
    },
    onDataSourceSelected: function(dsId) {
        var self = this;
        $.get(utils.builPath("schema/list"), {dsId: dsId}, function(result) {
            if(result.status !== 200) {
                if(console)
                    console.error(JSON.stringify(result));
                alert("加载data schema失败");
                return;
            }
            var list = [];
            result.data.forEach(function(e) {
                // 这里都使用text，后台使用text直接查询
                list.push({value:e.schemaName, text: e.schemaName});
            });
            self.state.schemaOpts = list;
            self.trigger(self.state);
        });
    },
    onCloseDialog: function() {
        this.state.dialog.show = false;
        this.trigger(this.state);
    },
    onOpenDialogByKey: function(key, obj) {
        var content=String(obj[key]);
        content = content.replace(/\n/gm, "<br/>");
        content = content.replace(/[' ']/gm, "&nbsp;");
        this.state.dialog.show = true;
        this.state.dialog.content = content;
        this.state.dialog.identity = key ;
        this.trigger(this.state);
    },
});

store.actions = actions;
module.exports = store;
