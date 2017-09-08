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
  handleSubmit:function(){
    var formdata={
      dsName:ReactDOM.findDOMNode(this.refs.name).value,
      dsType:ReactDOM.findDOMNode(this.refs.type).value,
      status:ReactDOM.findDOMNode(this.refs.status).value,
      dsDesc:ReactDOM.findDOMNode(this.refs.desc).value,
      topic:ReactDOM.findDOMNode(this.refs.name).value,
      ctrlTopic:ReactDOM.findDOMNode(this.refs.name).value + "_ctrl",
      schemaTopic:ReactDOM.findDOMNode(this.refs.name).value + "_schema",
      splitTopic:ReactDOM.findDOMNode(this.refs.name).value + "_topic",
      masterURL:$.trim(ReactDOM.findDOMNode(this.refs.masterurl).value),
      slaveURL:$.trim(ReactDOM.findDOMNode(this.refs.slaveurl).value),
      dbusUser:ReactDOM.findDOMNode(this.refs.user).value,
      dbusPassword:ReactDOM.findDOMNode(this.refs.pwd).value
    }
    if(formdata.dsName&&formdata.dsDesc&&formdata.masterURL&&formdata.slaveURL&&formdata.dbusUser&&formdata.dbusPassword){

        store.actions.handleSubmit(formdata, this.handleNext);
        /*
         var r=confirm("进入下一步");
         if (r==true){
         //alert("test:  "+this.state.id);
         //this.props.history.pushState({dsId:this.state.id}, "/nds-step/nds-second");
         this.handleNext();
         } else{
         this.props.history.pushState(null, "/nds-step");
         }
         */
        //alert("Step1-dsId：" + this.state.id);
        //this.props.history.pushState({dsId:18,dsType:"mysql",dsName:"mydb"}, "/nds-step/nds-second");

    }else{
      alert("表单存在必填项未填！！");
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
  handleUserBlur:function(){
    var type = ReactDOM.findDOMNode(this.refs.type).value;
    var dsName = ReactDOM.findDOMNode(this.refs.name).value;
    var user = ReactDOM.findDOMNode(this.refs.user).value;
    if(type  == "oracle"){
      ReactDOM.findDOMNode(this.refs.srctopic).value=dsName+"."+user.toUpperCase();
      ReactDOM.findDOMNode(this.refs.targettopic).value=dsName+"."+user.toUpperCase()+".result";
    }else{
      ReactDOM.findDOMNode(this.refs.srctopic).value=dsName+"."+user;
      ReactDOM.findDOMNode(this.refs.targettopic).value=dsName+"."+user+".result";
    }

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
  handleNext:function(id){
    this.props.history.pushState({dsId:id,dsType:ReactDOM.findDOMNode(this.refs.type).value,dsName:ReactDOM.findDOMNode(this.refs.name).value}, "/nds-step/nds-second");
  },
  validateAndSubmit:function(){
     utils.showLoading();
     this.handleURL(this.handleNameBlur,this.handleSubmit);
  },
  render: function() {
    var statusMap={'nds-first':'active','nds-second':'normal','nds-zkConf':'normal','nds-forth':'normal'};
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
      <select className="form-control" ref="type" >
        {global.isDebug ? (<option value="oracle">oracle</option>) : (null)}
        <option value="mysql">mysql</option>
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

      <div className="form-group">
      <label htmlFor="inputUser" className="col-sm-2 control-label">User   <span style={{color:"red"}}>*</span> </label>
      <div className="col-sm-10">
      <input  className="form-control"  placeholder="User" ref="user" onBlur={this.handleUserBlur}/>
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


    <div className="form-group">
      <div className="col-sm-offset-2 col-sm-10">
      <button type="button" className="btn btn-warning" onClick={this.validateAndSubmit}>Next</button>
    </div>
    </div>

    </form>
    <form className="form-horizontal" role="form" >

      <div className="form-group">
      <label htmlFor="inputTopic" className="col-sm-2 control-label">Topic</label>
      <div className="col-sm-10">
      <input  className="form-control"  ref="topic"  disabled="true"/>
      </div>
      </div>

      <div className="form-group">
      <label htmlFor="inputCtrl" className="col-sm-2 control-label">CtrlTopic</label>
      <div className="col-sm-10">
      <input  className="form-control"  ref="ctrltopic"  disabled="true"/>
      </div>
      </div>

      {global.isDebug
          ?
          (<div className="form-group">
            <label htmlFor="inputSchema" className="col-sm-2 control-label">SchemaTopic</label>
            <div className="col-sm-10">
              <input className="form-control" ref="schematopic" disabled="true"/>
            </div>
          </div>)
          :
          (null)}

      <div className="form-group">
      <label htmlFor="inputSplit" className="col-sm-2 control-label">SplitTopic</label>
      <div className="col-sm-10">
      <input  className="form-control"  ref="splittopic"  disabled="true"/>
      </div>
      </div>
      </form>

      <form className="form-horizontal" role="form" >

      <div className="form-group">
      <label htmlFor="inputSrc" className="col-sm-2 control-label">SrcTopic</label>
      <div className="col-sm-10">
      <input  className="form-control"  ref="srctopic"  disabled="true"/>
      </div>
      </div>

      <div className="form-group">
      <label htmlFor="inputTarget" className="col-sm-2 control-label">TargetTopic</label>
      <div className="col-sm-10">
      <input  className="form-control"  ref="targettopic"  disabled="true"/>
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
