var Reflux = require('reflux');
var $ = require('jquery')
var utils = require('../common/utils');

var actions = Reflux.createActions(['initialLoad','dataSourceSelected','search','handleSubmit',
    'openDialogByKey','closeDialog','deleteSchema']);

var store = Reflux.createStore({
    state: {
        dsOptions: [],
        data: [],
        currentPageNum: 1,
        dialog: {
            show: false,
            content:"",
            identity:""
        },
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
    onHandleSubmit:function(formdata,callback){
        var self = this;
        $.get(utils.builPath("schema2/update"), formdata, function(result) {
            if(result.status !== 200) {
                alert("修改schema失败");
                return;
            }
            self.trigger(self.state);
            callback();
        });
    },
    onDeleteSchema:function(param, search){
        var self = this;
        $.get(utils.builPath("schema2/deleteSchema"), param, function(result) {
            if(result.status !== 200) {
                alert("Delete schema failed");
                return;
            }
            search(null, self.state.currentPageNum);
        });
    },
    //监听所有的actions
    listenables: [actions],
    onSearch: function(p){
        var self = this;
        $.get(utils.builPath("schema2/search"), p, function(result) {
            self.state.currentPageNum = p.pageNum;
            if(result.status !== 200) {
                if(console)
                    console.error(JSON.stringify(result));
                alert("加载Schema失败");
                return;
            }
            if(console.log("onsearch method called"));
            self.state.data = result.data;
            self.trigger(self.state);
            utils.hideLoading();
        });
    }
});

store.actions = actions;
module.exports = store;
