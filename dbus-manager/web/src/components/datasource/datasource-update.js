var React = require('react');
var ReactDOM = require('react-dom');
var Reflux = require('reflux');
var B = require('react-bootstrap');
var Tab = require('fixed-data-table');
var TF = require('../common/table/tab-frame');
var Select = require('../common/select');
var Table = require('../common/table/default-table');
var store = require('./datasource-store');

var ReactDOM = require('react-dom');

var cells = require('../common/table/cells');
var Column = Tab.Column;
var Cell = Tab.Cell;
var TextCell = cells.TextCell;

var UpdateDs = React.createClass({
   mixins: [Reflux.listenTo(store, "_onStatusChange")],
    getInitialState: function() {
        var state = store.initState();
        return state;
    },
    componentDidMount: function() {
        store.actions.initialLoad();
        ReactDOM.findDOMNode(this.refs.desc).value = this.props.location.state.passParam.desc;
    },

    _onStatusChange: function(state) {
        this.setState(state);
    },
    handleSubmit:function(){
    	var formdata={
      id:this.props.location.state.passParam.id,
      dsDesc:ReactDOM.findDOMNode(this.refs.desc).value,
			masterURL:ReactDOM.findDOMNode(this.refs.masterurl).value,
			slaveURL:ReactDOM.findDOMNode(this.refs.slaveurl).value,
            dsPartition:ReactDOM.findDOMNode(this.refs.dsPartition).value,
		  };
      var selectStatus = ReactDOM.findDOMNode(this.refs.status).value;
      if(selectStatus != "default"){
        store.actions.handleSubmit(formdata,this.comeBack);
      }else{
        alert("please select status !");
      }
    },
    comeBack:function(){
      this.props.history.push({passParam: "come back"}, "/");
    },
    startOrStop:function(){
        var selectStatus = ReactDOM.findDOMNode(this.refs.status).value;
        if(selectStatus == "active"){
            var startParam = {
            id:this.props.location.state.passParam.id,
            status:"active"
            };
            var validateParam = {
             dsType:this.props.location.state.passParam.dsType,
             masterURL:ReactDOM.findDOMNode(this.refs.masterurl).value,
             slaveURL:ReactDOM.findDOMNode(this.refs.slaveurl).value,
             user:this.props.location.state.passParam.user,
             password:this.props.location.state.passParam.password
            };
            store.actions.start(startParam,validateParam);
        }else if(selectStatus == "inactive"){
           var stopParam ={
            id:this.props.location.state.passParam.id,
            status:"inactive"
           };
           store.actions.stop(stopParam);
        }
    },
    render: function() {
        var masterURL = this.props.location.state.passParam.masterURL;
        var slaveURL = this.props.location.state.passParam.slaveURL;
        var dsName = this.props.location.state.passParam.dsName;
        var dsPartition = this.props.location.state.passParam.dsPartition;
        return (
            <div className="container-fluid">
                <div className="row header">
                    <h4 className="col-xs-12">update datasource</h4>
                    <h4 className="col-xs-12">{dsName}</h4>
                </div>
                <div className="row body" >
                    <div className="col-xs-6">
                     <form className="form-horizontal" role="form" >

                       <div className="form-group ">
                        <label for="inputDesc" className="col-sm-2 control-label " >Desc:</label>
                        <div className="col-sm-10 ">
                         <input className="form-control" ref="desc"/>
                        </div>
                       </div>


                         <div className="form-group ">
                                <label for="areaMaster" className="col-sm-2 control-label">Master_URL:</label>
                                <div className="col-sm-10">
                                  <textarea className="form-control" rows="4" ref="masterurl" style={{resize:"none"}}>{masterURL}</textarea>
                                </div>
                         </div>

                         <div className="form-group ">
                             <label for="areaSlave" className="col-sm-2 control-label">Slave_URL:</label>
                             <div className="col-sm-10 ">
                                 <textarea className="form-control" rows="4" ref="slaveurl" style={{resize:"none"}}>{slaveURL}</textarea>
                             </div>
                         </div>

                         <div className="form-group ">
                             <label for="areaDsPartition" className="col-sm-2 control-label">dsPartition:</label>
                             <div className="col-sm-10 ">
                                 <textarea className="form-control" rows="2" ref="dsPartition" style={{resize:"none"}}>{dsPartition}</textarea>
                             </div>
                         </div>


                        <div className="form-group">
                           <label for="selectStatus" className="col-sm-2 control-label">Status:</label>
                        <div className="col-sm-10">
                         <select className="form-control" ref="status" onChange={this.startOrStop}>
                              <option value="default">select status</option>
                              <option value="active">active</option>
                              <option value="inactive">inactive</option>
                         </select>
                        </div>
                       </div>
                          <div className="form-group ">
                           <div className="col-sm-offset-2 col-sm-10">
                             <button type="button" className="btn btn-primary"  onClick={this.handleSubmit}>Save</button>
                              &nbsp;&nbsp;&nbsp;&nbsp;
                             <button type="button" className="btn btn-default" onClick={this.comeBack}>Back</button>
                           </div>
                         </div>
                       </form>
                        </div>
                   </div>
            </div>
        );
    }
});

module.exports = UpdateDs;
