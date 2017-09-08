var Reflux = require('reflux');
var $ = require('jquery')
var utils = require('../../common/utils');

var actions = Reflux.createActions(['initialLoad','handleSubmit','handleNameBlur','handleURL']);

var store = Reflux.createStore({
    //监听所有的actions
    listenables: [actions],
    state: {
        style:"",
        data: {}
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
                if(global.isDebug) {
                    alert("URL不可用(请注意用户名、密码以及url是否正确，type是oracle还是mysql)");
                } else {
                    alert("URL不可用(请注意用户名、密码以及url是否正确)");
                }

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

      /*
      $.ajax({
              url: utils.builPath("ds/add"),
              type:'GET',
              dataType: 'json',
              cache: false,
              data: formdata,
              async: false,
              success: function(result) {
                alert(result.id);
                if(result.status !== 200) {
                  alert("添加数据源失败");
                  return;
                }
                self.state.id = result.id;
                self.trigger(self.state);
              }.bind(this),
              error: function(res) {
              }
            });
       */
    }
});

store.actions = actions;
module.exports = store;
