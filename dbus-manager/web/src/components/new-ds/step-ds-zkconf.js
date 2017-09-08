var React = require('react')
var Reflux = require('reflux')
var $ = require('jquery')
var B = require('react-bootstrap')
var store = require('./step-ds-zkconf/store')
var StepCursor = require('./step-cursor')
var ZkTree = require('../common/tree/zk-tree')
var FreeScrollBar = require('react-free-scrollbar')
var styles = require('../common/tree/styles')

var DsZkConf = React.createClass({
  mixins: [Reflux.listenTo(store, '_onStatusChange')],
  getInitialState: function () {
    var state = store.initState();
    this.passParam();
    return state;
  },
  componentDidMount: function () {
    store.actions.initialLoad();
  },
  _onStatusChange: function (state) {
    this.setState(state);
  },
  // 下一步
  nextStep: function () {
    console.log("this.state.dsName: " + this.state.dsType);
    this.props.history.pushState({ dsName:this.state.dsName,dsType:this.state.dsType}, '/nds-step/nds-start-topos');
  },
  passParam:function () {
    var params = this.props.location.state;
    store.actions.passParam(params);
  },
  cloneFromTemplates:function () {
    store.actions.cloneFromTemplates();
  },
  render: function () {
    var zkConfTree = this.state.data || {}
    var statusMap = { 'nds-first':'finished', 'nds-second':'finished', 'nds-zkConf':'active', 'nds-forth':'normal' }
    return (<div className='container-fluid'>
      <div className='row header'>
        <h4 className='col-xs-12'>Zookeeper Config Management</h4>
      </div>
      <div className="row header"><div className="col-xs-9">
        <StepCursor currentStep={2}/><br/>
      </div></div>
      <div className='row body'>
        <button type='button' className='btn btn-default' onClick={this.cloneFromTemplates} style={{ marginTop:'16px', marginLeft:'20px' }}>Clone ZkConf From Templates</button><br />
        <B.Button
          bsSize='sm'
          bsStyle='warning' style={{ float:'right', marginRight:'68px' }}
          onClick={this.nextStep}>
      Next
      </B.Button>
          <ZkTree data={zkConfTree} node={this.state.cursor} />
      </div>
    </div>)
  }
})

module.exports = DsZkConf
