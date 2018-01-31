var Reflux = require('reflux');
var $ = require('jquery')
var utils = require('../common/utils');

var actions = Reflux.createActions(['search','closeDialog', 'openDialogByKey']);

var store = Reflux.createStore({
    state: {
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
    //监听所有的actions
    listenables: [actions],
    onSearch: function(p){
        var self = this;
        $.get(utils.builPath("fullpullHistory/search"), p, function(result) {
            if(result.status !== 200) {
                if(console)
                    console.error(JSON.stringify(result));
                alert("查询全量历史失败");
                return;
            }
            self.state.data = result.data;
            self.trigger(self.state);
            utils.hideLoading();
        });
    }
});

store.actions = actions;
module.exports = store;
