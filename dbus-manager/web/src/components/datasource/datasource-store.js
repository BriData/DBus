var Reflux = require('reflux');
var $ = require('jquery')
var utils = require('../common/utils');

var actions = Reflux.createActions(['search','start','stop',
    'closeDialog', 'openDialogByKey' , 'handleSubmit','deleteDataSource']);

var store = Reflux.createStore({
    state: {
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
    onStart:function(startParam,validateParam){
        var self = this;
        $.get(utils.builPath("ds/validate"), validateParam, function(result) {
            if(result.data !== 1) {
                alert("URL不可用");
                return;
            }

              $.get(utils.builPath("ds/active"), startParam,function(result) {
              if(result.status !== 200) {
                if(console)
                    console.error(JSON.stringify(result));
                alert("启动失败");
                return;
              }
              /*
               self.state.data.list.forEach(function(e){
                if(e.id == startParam.id){
                e.status = "active";
                }
               });
              self.trigger(self.state);
              */
              });

        });


       //this.onSearch({});

    },
    onStop:function(stopParam){
        var self = this;
        $.get(utils.builPath("ds/inactive"), stopParam ,function(result) {
            if(result.status !== 200) {
                if(console)
                    console.error(JSON.stringify(result));
                alert("停止失败");
                return;
            }
            /*
            self.state.data.list.forEach(function(e){
              if(e.id == stopParam.id){
                e.status = "inactive";
            }
            });
            self.trigger(self.state);
            */
        });

        //this.onSearch({});
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
        $.get(utils.builPath("ds/updateDs"), formdata, function(result) {
            if(result.status !== 200) {
                alert("修改数据源失败");
                return;
            }
            self.trigger(self.state);
            callback();
        });
    },
    onDeleteDataSource:function(param,search){
        var self = this;
        $.get(utils.builPath("ds/deleteDataSource"), param, function(result) {
            if(result.status !== 200) {
                alert("Delete datasource failed");
                return;
            }
            search(null, self.state.currentPageNum);
        });
    },
    //监听所有的actions
    listenables: [actions],
    onSearch: function(p){
        var self = this;
        $.get(utils.builPath("ds/search"), p, function(result) {
            self.state.currentPageNum = p.pageNum;
            if(result.status !== 200) {
                if(console)
                    console.error(JSON.stringify(result));
                alert("查询数据源失败");
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
