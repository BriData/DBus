var Reflux = require('reflux');
var $ = require('jquery')
var utils = require('../utils');

var actions = Reflux.createActions(['initialLoad','logout']);

var store = Reflux.createStore({
    state: {
    },
    initState: function() {
        return this.state;
    },
    onInitialLoad: function() {
    },
    listenables: [actions],
    onLogout: function(callback){
        var self = this;
        var flag = 0;
        $.get(utils.builPath("/logout"),function(result) {
            if(result.status !== 200) {
                if(console)
                    console.error(JSON.stringify(result));
                alert("logout failed!");
                return;
            }
            else{
                flag = 1;
                callback(flag);
            }
        });
    }
});
       
store.actions = actions;
module.exports = store;
