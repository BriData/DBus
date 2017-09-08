var Reflux = require('reflux');
var $ = require('jquery');
var utils = require('../common/utils');

var actions = Reflux.createActions(['initialLoad','handleSubmit','addChildNode', 'deleteNode']);

var store = Reflux.createStore({
    //监听所有的actions
    listenables: [actions],
    state: {
      data:{name:'Loading......',
        path:'/',
        content:'',
        children:[],
        toggled:false,
        existed:true
      }
    },

    initState: function() {
        return this.state;
    },

    onInitialLoad: function() {
        var self = this;
        $.get(utils.builPath("zk/loadLevelOfPath"), {path:'/'}, function(result) {
            if(result.status !== 200) {
                // self.state.data = [{name:'加载失败！'}];
                return;
            }
            self.state.data=result.data;
            self.trigger(self.state);
        });
    },

    onAddChildNode:function(expandPath){
        var self = this;
        $.get(utils.builPath("zk/loadLevelOfPath"), {path:expandPath}, function(result) {
            if(result.status !== 200) {
                return;
            }
            //找到expandPath路径的结点下，修改其children
            let parentName = expandPath.split("/");
            let parent = self.state.data;
            for(let index=1; index < parentName.length; index++){
                if(parent.children != null){
                    for(let childIndex=0; childIndex < parent.children.length; childIndex++){
                        if(parent.children[childIndex].name === parentName[index]){
                            parent = parent.children[childIndex];
                            break;
                        }
                    }
                }
            }
            parent.children = result.data.children;
            self.trigger(self.state);
        });
    },
    onDeleteNode:function(expandPath){
        var self = this;
        $.get(utils.builPath("zk/deleteZkNodeOfPath"), {path:expandPath}, function(result) {
            if(result.status !== 200) {
                alert("删除结点失败");
                return;
            }
            alert("删除结点成功");
            //self.trigger(self.state);
            let parentPath = expandPath.substr(0, expandPath.lastIndexOf("/"));
            $.get(utils.builPath("zk/loadLevelOfPath"), {path:parentPath}, function(result) {
                if(result.status !== 200) {
                    return;
                }
                //找到expandPath路径的结点下，修改其children
                let parentName = parentPath.split("/");
                let parent = self.state.data;
                for(let index=1; index < parentName.length; index++){
                    if(parent.children != null){
                        for(let childIndex=0; childIndex < parent.children.length; childIndex++){
                            if(parent.children[childIndex].name === parentName[index]){
                                parent = parent.children[childIndex];
                                break;
                            }
                        }
                    }
                }
                parent.children = result.data.children;
                self.trigger(self.state);
            });
        });
    },

    onHandleSubmit:function(dsName){
        var self = this;
        $.get(utils.builPath("zk/cloneConfFromTemplate"), dsName, function(result) {
            if(result.status !== 200) {
                alert("添加数据源失败");
                return;
            }
            self.trigger(self.state);
        });
    }
});

store.actions = actions;
module.exports = store;
