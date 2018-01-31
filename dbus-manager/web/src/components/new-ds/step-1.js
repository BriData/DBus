var React = require('react');
var Reflux = require('reflux');
var B = require('react-bootstrap');
var Tab = require('fixed-data-table');
var TF = require('../common/table/tab-frame');
var Select = require('../common/select');
var Table = require('../common/table/default-table');
var store = require('./step-1/store');
var StepCursor = require('./step-cursor');

var ReactDOM = require('react-dom');
var utils = require('../common/utils');

var cells = require('../common/table/cells');
var Column = Tab.Column;
var Cell = Tab.Cell;
var TextCell = cells.TextCell;

var $ = require('jquery')
var Modal = B.Modal;
var StepFirst = React.createClass({
  mixins: [Reflux.listenTo(store, "_onStatusChange")],
  getInitialState: function() {
    var state = store.initState();
    return state;
  },
  componentDidMount: function() {
    store.actions.initialLoad();
  },

  _onStatusChange: function(state) {
    this.setState(state);
  },
  onTypeChange: function(event) {
    store.actions.typeChange(event.target.value);
  },
  handleSubmit:function(){
    var type = ReactDOM.findDOMNode(this.refs.type).value;
    if(type == utils.dsType.logLogstash 
        || type == utils.dsType.logLogstashJson
        || type == utils.dsType.logUms
        || type == utils.dsType.logFlume
        || type == utils.dsType.logFilebeat) {
      var formdata={
        dsName:ReactDOM.findDOMNode(this.refs.name).value,
        dsType:ReactDOM.findDOMNode(this.refs.type).value,
        status:ReactDOM.findDOMNode(this.refs.status).value,
        dsDesc:ReactDOM.findDOMNode(this.refs.desc).value,
        topic:ReactDOM.findDOMNode(this.refs.topic).value,
        ctrlTopic:ReactDOM.findDOMNode(this.refs.ctrltopic).value,
        schemaTopic:ReactDOM.findDOMNode(this.refs.schematopic).value,
        splitTopic:ReactDOM.findDOMNode(this.refs.splittopic).value,
        masterURL:"empty",
        slaveURL:"empty",
        dbusUser:"empty",
        dbusPassword:"empty"
      }
      if(formdata.dsName&&formdata.dsDesc){
        store.actions.handleSubmit(formdata, this.handleNext);
      }else{
        alert("表单存在必填项未填！！");
      }
    } else {
      var formdata={
        dsName:ReactDOM.findDOMNode(this.refs.name).value,
        dsType:ReactDOM.findDOMNode(this.refs.type).value,
        status:ReactDOM.findDOMNode(this.refs.status).value,
        dsDesc:ReactDOM.findDOMNode(this.refs.desc).value,
        topic:ReactDOM.findDOMNode(this.refs.topic).value,
        ctrlTopic:ReactDOM.findDOMNode(this.refs.ctrltopic).value,
        schemaTopic:ReactDOM.findDOMNode(this.refs.schematopic).value,
        splitTopic:ReactDOM.findDOMNode(this.refs.splittopic).value,
        masterURL:$.trim(ReactDOM.findDOMNode(this.refs.masterurl).value),
        slaveURL:$.trim(ReactDOM.findDOMNode(this.refs.slaveurl).value),
        dbusUser:ReactDOM.findDOMNode(this.refs.user).value,
        dbusPassword:ReactDOM.findDOMNode(this.refs.pwd).value
      }
      if(formdata.dsName&&formdata.dsDesc&&formdata.masterURL&&formdata.slaveURL&&formdata.dbusUser&&formdata.dbusPassword){
        store.actions.handleSubmit(formdata, this.handleNext);
      }else{
        alert("表单存在必填项未填！！");
      }
    }
  },
  handleNameTopic:function(){
    var dsName = ReactDOM.findDOMNode(this.refs.name).value;
    ReactDOM.findDOMNode(this.refs.topic).value=dsName;
    ReactDOM.findDOMNode(this.refs.ctrltopic).value=dsName+"_ctrl";
    ReactDOM.findDOMNode(this.refs.schematopic).value=dsName+"_schema";
    ReactDOM.findDOMNode(this.refs.splittopic).value=dsName+"_split";
  },
  handleNameBlur:function(handleSubmit){
    var dsName = ReactDOM.findDOMNode(this.refs.name).value;
    var name = {
      dsName:dsName
    }
    store.actions.handleNameBlur(name,handleSubmit);
  },
  handleURL:function(handleNameBlur,handleSubmit){
    var url ={
      dsType:ReactDOM.findDOMNode(this.refs.type).value,
      masterURL:$.trim(ReactDOM.findDOMNode(this.refs.masterurl).value),
      slaveURL:$.trim(ReactDOM.findDOMNode(this.refs.slaveurl).value),
      user:ReactDOM.findDOMNode(this.refs.user).value,
      password:ReactDOM.findDOMNode(this.refs.pwd).value
    }
    store.actions.handleURL(url,handleNameBlur,handleSubmit);
  },
  handleNext: function (id) {
    var dsType = ReactDOM.findDOMNode(this.refs.type).value;
    var dsName = ReactDOM.findDOMNode(this.refs.name).value;
    if(dsType == utils.dsType.logLogstash 
        || dsType == utils.dsType.logLogstashJson 
        || dsType == utils.dsType.logUms
        || dsType == utils.dsType.logFlume
        || dsType == utils.dsType.logFilebeat) {
      this.props.history.pushState({
        dsId: id,
        dsType: dsType,
        dsName: dsName
      }, "/nds-step/nds-second-log-processor");
    } else if(dsType == utils.dsType.mysql
        || dsType == utils.dsType.oracle
        || dsType == utils.dsType.mongo) {
      this.props.history.pushState({
        dsId: id,
        dsType: dsType,
        dsName: dsName
      }, "/nds-step/nds-second");
    }

  },
  validateAndSubmit:function(){
    var dsType = ReactDOM.findDOMNode(this.refs.type).value;
    if(dsType == utils.dsType.logLogstash 
        || dsType == utils.dsType.logLogstashJson
        || dsType == utils.dsType.logUms
        || dsType == utils.dsType.logFlume
        || dsType == utils.dsType.logFilebeat) {
      this.handleNameBlur(this.handleSubmit);
    } else {
      this.handleURL(this.handleNameBlur,this.handleSubmit);
    }

  },
  render: function() {
    return (
        <div className="container-fluid">
          <div className="row header">
            <h4 className="col-xs-12">New DataLine</h4>
          </div>
          <div className="row header"><div className="col-xs-9">
            <StepCursor currentStep={0}/><br/>
          </div></div>
          <div className="row body" >
            <div className="col-xs-8">
              <form className="form-horizontal" role="form" >

                <div className="form-group">
                  <label htmlFor="inputDsName" className="col-sm-2 control-label"  >DsName  <span style={{color:"red"}}>*</span> </label>
                  <div className="col-sm-10">
                    <input  className="form-control"  placeholder="Please input dataSource name..." ref="name"   onBlur={this.handleNameTopic} />
                  </div>
                </div>

                <div className="form-group">
                  <label htmlFor="selectType" className="col-sm-2 control-label">Type   <span style={{color:"red"}}>*</span> </label>
                  <div className="col-sm-10">
                    <select className="form-control" ref="type" onChange={this.onTypeChange}>
                      <option value={utils.dsType.oracle}>{utils.dsType.oracle}</option>
                      <option value={utils.dsType.mysql}>{utils.dsType.mysql}</option>
                      <option value={utils.dsType.logLogstash}>{utils.dsType.logLogstash}</option>
                      <option value={utils.dsType.logLogstashJson}>{utils.dsType.logLogstashJson}</option>
                      <option value={utils.dsType.logUms}>{utils.dsType.logUms}</option>
                      <option value={utils.dsType.logFlume}>{utils.dsType.logFlume}</option>
                      <option value={utils.dsType.logFilebeat}>{utils.dsType.logFilebeat}</option>
                      {global.isDebug ? <option value={utils.dsType.mongo}>{utils.dsType.mongo}</option>:null}
                    </select>
                  </div>
                </div>

                <div className="form-group">
                  <label htmlFor="selectStatus" className="col-sm-2 control-label">Status   <span style={{color:"red"}}>*</span> </label>
                  <div className="col-sm-10">
                    <select className="form-control" ref="status">
                      <option value="active">active</option>
                      <option value="inactive">inactive</option>
                    </select>
                  </div>
                </div>

                <div className="form-group">
                  <label htmlFor="inputDesc" className="col-sm-2 control-label">Desc   <span style={{color:"red"}}>*</span> </label>
                  <div className="col-sm-10">
                    <input  className="form-control"   ref="desc" />
                  </div>
                </div>

                {this.state.isHideJdbcInfo?null:
                    (<div>
                      <div className="form-group">
                        <label htmlFor="inputUser" className="col-sm-2 control-label">User   <span style={{color:"red"}}>*</span> </label>
                        <div className="col-sm-10">
                          <input  className="form-control"  placeholder="User" ref="user"/>
                        </div>
                      </div>

                      <div className="form-group">
                        <label htmlFor="inputPassword" className="col-sm-2 control-label">Password   <span style={{color:"red"}}>*</span> </label>
                        <div className="col-sm-10">
                          <input type="password" className="form-control" placeholder="Password" ref="pwd" />
                        </div>
                      </div>

                      <div className="form-group">
                        <label htmlFor="areaMaster" className="col-sm-2 control-label">MasterURL   <span style={{color:"red"}}>*</span> </label>
                        <div className="col-sm-10">
                          <textarea  className="form-control" rows="6" ref="masterurl"  style={{resize:"none"}}></textarea>
                        </div>
                      </div>

                      <div className="form-group">
                        <label htmlFor="areaSlave" className="col-sm-2 control-label">SlaveURL   <span style={{color:"red"}}>*</span> </label>
                        <div className="col-sm-10">
                          <textarea className="form-control" rows="6" ref="slaveurl" style={{resize:"none"}}></textarea>
                        </div>
                      </div>

                    </div>)
                }


                <div className="form-group">
                  <div className="col-sm-offset-2 col-sm-10">
                    <button type="button" className="btn btn-warning" onClick={this.validateAndSubmit}>Next</button>
                  </div>
                </div>

              </form>
              <form className="form-horizontal" role="form" >

                <div className="form-group">
                  <label htmlFor="inputTopic" className="col-sm-2 control-label">Topic   <span style={{color:"red"}}>*</span> </label>
                  <div className="col-sm-10">
                    <input  className="form-control"  ref="topic"  />
                  </div>
                </div>

                <div className="form-group">
                  <label htmlFor="inputCtrl" className="col-sm-2 control-label">CtrlTopic   <span style={{color:"red"}}>*</span> </label>
                  <div className="col-sm-10">
                    <input  className="form-control"  ref="ctrltopic"  />
                  </div>
                </div>

                  <div className="form-group">
                    <label htmlFor="inputSchema" className="col-sm-2 control-label">SchemaTopic   <span style={{color:"red"}}>*</span> </label>
                    <div className="col-sm-10">
                      <input className="form-control" ref="schematopic" />
                    </div>
                  </div>

                <div className="form-group">
                  <label htmlFor="inputSplit" className="col-sm-2 control-label">SplitTopic   <span style={{color:"red"}}>*</span> </label>
                  <div className="col-sm-10">
                    <input  className="form-control"  ref="splittopic"  />
                  </div>
                </div>
              </form>

            </div>
          </div>

        </div>
    );
  }
});

module.exports = StepFirst;
