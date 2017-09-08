var Reflux = require('reflux');
var $ = require('jquery')
var utils = require('../common/utils');

var actions = Reflux.createActions(['initialLoad']);

var store = Reflux.createStore({
    state: {
    },
    initState: function() {
        return this.state;
    },
    onInitialLoad: function() {
        //this.onSearch({});
    },
    //监听所有的actions
    listenables: [actions],
});

store.actions = actions;
module.exports = store;