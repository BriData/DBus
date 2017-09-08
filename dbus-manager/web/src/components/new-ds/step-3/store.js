var Reflux = require('reflux');
var $ = require('jquery')
var utils = require('../common/utils');

var actions = Reflux.createActions(['initialLoad']);

var store = Reflux.createStore({
    //监听所有的actions
    listenables: [actions],
    state: {
        dsOptions: [],
        schemaOpts: [],
        data: []
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
        this.onSearch({});
    }
});

store.actions = actions;
module.exports = store;
