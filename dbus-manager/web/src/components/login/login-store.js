var Reflux = require('reflux');
var $ = require('jquery')
var utils = require('../common/utils');

var actions = Reflux.createActions(['initialLoad','login']);

var store = Reflux.createStore({
    state: {
    },
    initState: function() {
        return this.state;
    },
    onInitialLoad: function() {
    },
    //监听所有的actions
    listenables: [actions],
    login: function(user,password,callback) {
        var flag = 0;
        var p={userName:user,pwd:password};
        $.get(utils.builPath("/login"), p, function(result) {
            if(result.status !== 200) {
                alert("login failed!");
                return;
            }else{
                flag = 1;
                callback(flag);
            }
            
        });
        
    }
});

store.actions = actions;
module.exports = store;