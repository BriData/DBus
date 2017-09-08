var Reflux = require('reflux');
var $ = require('jquery');
var utils = require('../utils');

var actions = Reflux.createActions(['initialLoad','addNode','deleteNode','getNodeInfo', 'setExpandedKeys','setSelectedKeys','setCurrentNode']);

var store = Reflux.createStore({
  listenables: [actions],
  state: {
      defaultExpandedKeys:['.$/'],
      expandedKeys: ['.$/'],
      selectedKeys: '.$/DBus',
      currentNode:''
  },
  initState: function() {
    return this.state;
  },
  onInitialLoad: function() {
      var self = this;
      self.state.expandedKeys = ['.$/'];
      self.state.selectedKeys = '.$/DBus';
      self.state.currentNode = '';
      self.trigger(self.state);
  },
  onAddNode:function(currentPath, nodeName, refreshNode){
      var self = this;
      $.post(utils.builPath("zk/addZkNodeOfPath"), {path:currentPath, nodeName:nodeName}, function(result) {
          if(result.status !== 200) {
              alert("添加结点失败");
              return;
          }
          alert("添加结点成功");
          //self.trigger(self.props.state);
          refreshNode(currentPath)
      });
  },
  onDeleteNode:function(currentPath){
      var self = this;
      $.get(utils.builPath("zk/deleteZkNodeOfPath"), {path:currentPath}, function(result) {
          if(result.status !== 200) {
              alert("删除结点失败");
              return;
          }
          alert("删除结点成功");
          //self.trigger(self.props.state);
          //let parentPath = currentPath.substr(0, currentPath.lastIndexOf("/"));
      });
  },

  onGetNodeInfo:function(currentPath){
        var self = this;
        console.log(currentPath);
        $.get(utils.builPath("zk/getZkNodeOfPath"), {path:currentPath}, function(result) {
            if(result.status !== 200) {
                alert("获取结点信息失败");
                return;
            }
            console.log(self.state);
            self.state.currentNodeInfo= {currentNodePath:currentPath, currentNodeData: result.data};
            self.trigger(self.state);
        });
    },

    onSetExpandedKeys:function (expandedKeys) {
        var self = this;
        self.state.expandedKeys = [...expandedKeys];
        self.trigger(self.state);
    },
    onSetSelectedKeys:function (selectedKeys) {
        var self = this;
        self.state.selectedKeys = selectedKeys;
        self.trigger(self.state);
    },
    onSetCurrentNode:function (currentNode) {
        var self = this;
        self.state.currentNode = currentNode;
        self.trigger(self.state);
    }
});

store.actions = actions;
module.exports = store;
