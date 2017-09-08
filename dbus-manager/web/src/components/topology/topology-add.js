var React = require('react');
var ReactDOM = require('react-dom');
var Reflux = require('reflux');
var B = require('react-bootstrap');
var Tab = require('fixed-data-table');
var TF = require('../common/table/tab-frame');
var Select = require('../common/select');
var Table = require('../common/table/default-table');
var store = require('./topologys-store');

var ReactDOM = require('react-dom');

var cells = require('../common/table/cells');
var Column = Tab.Column;
var Cell = Tab.Cell;
var TextCell = cells.TextCell;

var AddTopology = React.createClass({
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
    	var formdata = {
      dsId:this.state.dsId,
      topologyName:ReactDOM.findDOMNode(this.refs.topologyName).value,
      jarName:this.state.jarName
		  };
      
      store.actions.handleSubmit(formdata,this.comeBack);           
    },
    comeBack:function(){
      this.props.history.pushState({passParam: "come back"}, "/topology"); 
    },
    jarList:function(dsId){
      var ds = {
        dsId:dsId
      };
      store.actions.jarList(ds);
    },
    selectJar:function(jarName){
      var jar = {
        jarName:jarName
      };
      store.actions.selectJar(jar);
    },
    render: function() {
        return (
            <div className="container-fluid">
                <div className="row header">
                    <h4 className="col-xs-12">add topology</h4>
                </div>
                <div className="row body" >
                    <div className="col-xs-6">
                     <form className="form-horizontal" role="form" >

                        <div className="form-group">
                           <label for="selectStatus" className="col-sm-3 control-label">dsName:</label>
                        <div className="col-sm-9">
                         <Select
                           ref="ds"
                           defaultOpt={{value:0, text:"select a data source"}}
                           options={this.state.dsOptions}
                           onChange={this.jarList}/>
                        </div>
                       </div>


                       <div className="form-group ">
                        <label for="inputDescription" className="col-sm-3 control-label " >topologyName:</label>
                        <div className="col-sm-9 ">
                         <input className="form-control" ref="topologyName"/>
                        </div>
                       </div>

                       <div className="form-group">
                           <label for="selectStatus" className="col-sm-3 control-label">jarName:</label>
                        <div className="col-sm-9">
                         <Select
                           ref="jarName"
                           defaultOpt={{value:0, text:"select jar "}}
                           options={this.state.jarOptions}
                           onChange={this.selectJar}/>
                        </div>
                       </div>


                          <div className="form-group ">
                           <div className="col-sm-offset-2 col-sm-10">
                             <button type="button" className="btn btn-default"  onClick={this.handleSubmit}>Submit</button>
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

module.exports = AddTopology;