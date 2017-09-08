var React = require('react');
var Reflux = require('reflux');
var store = require('./zk-manager-store');
var styles = require('../common/tree/styles');
var ZkTreeTest = require('../common/tree/zk-tree-test');

var ZkManager = React.createClass({
  mixins: [Reflux.listenTo(store, '_onStatusChange')],
  getInitialState: function () {
    var state = store.initState();
    return state
  },
  componentDidMount: function () {
    store.actions.initialLoad()
  },
  _onStatusChange: function (state) {
    this.setState(state)
  },
  loadchildNode: function(expandPath){
      //将childNode加入到原先已经加载的state中
      store.actions.addChildNode(expandPath);
  },
  deleteNode: function (expandPath) {
      store.actions.deleteNode(expandPath);
  },

  render: function () {
    var zkConfTree = this.state.data || [];
    return (
      <div className='container-fluid'>
        <div className='row header'>
          <h4 className='col-xs-12'>Zookeeper Manager</h4>
        </div>
        <div className='row text-center' />
        <div className='row body'>
            <ZkTreeTest data={zkConfTree} node={this.state.cursor} loadChildNode={this.loadchildNode} deleteNode={this.deleteNode}/>
        </div>
      </div>)
  }
});

module.exports = ZkManager
