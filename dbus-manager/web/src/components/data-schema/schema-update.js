var React = require('react');
var ReactDOM = require('react-dom');
var Reflux = require('reflux');
var B = require('react-bootstrap');
var Tab = require('fixed-data-table');
var TF = require('../common/table/tab-frame');
var Select = require('../common/select');
var Table = require('../common/table/default-table');
var store = require('./schema-store');

var ReactDOM = require('react-dom');

var cells = require('../common/table/cells');
var Column = Tab.Column;
var Cell = Tab.Cell;
var TextCell = cells.TextCell;

var UpdateSchema = React.createClass({
   mixins: [Reflux.listenTo(store, "_onStatusChange")],
    getInitialState: function() {
        var state = store.initState();
        return state;
    },
    componentDidMount: function() {
        store.actions.initialLoad();
        ReactDOM.findDOMNode(this.refs.description).value = this.props.location.state.passParam.description;
    },

    _onStatusChange: function(state) {
        this.setState(state);
    },
    handleSubmit:function(){
    	var formdata={
      dsId:this.props.location.state.passParam.dsId,
      schemaName:this.props.location.state.passParam.schemaName,
      status:ReactDOM.findDOMNode(this.refs.status).value,
      description:ReactDOM.findDOMNode(this.refs.description).value
		  };
      if(formdata.status == "default"){
         formdata.status = this.props.location.state.passParam.description;
      }
      store.actions.handleSubmit(formdata,this.comeBack);           
    },
    comeBack:function(){
      this.props.history.pushState({passParam: "come back"}, "/data-schema"); 
    },
    render: function() {
        var schemaName = this.props.location.state.passParam.schemaName;
        return (
            <div className="container-fluid">
                <div className="row header">
                    <h4 className="col-xs-12">update schema</h4>
                    <h4 className="col-xs-12">{schemaName}</h4>
                </div>
                <div className="row body" >
                    <div className="col-xs-6">
                     <form className="form-horizontal" role="form" >

                       <div className="form-group ">
                        <label for="inputDescription" className="col-sm-2 control-label " >description:</label>
                        <div className="col-sm-10 ">
                         <input className="form-control" ref="description"/>
                        </div>
                       </div>



                        <div className="form-group">
                           <label for="selectStatus" className="col-sm-2 control-label">Status:</label>
                        <div className="col-sm-10">
                         <select className="form-control" ref="status">
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

module.exports = UpdateSchema;