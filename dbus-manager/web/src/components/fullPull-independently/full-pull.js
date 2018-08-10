var React = require('react');
var ReactDOM = require('react-dom');
var Reflux = require('reflux');
var B = require('react-bootstrap');
var store = require('./fp-store');
var JSONEditor = require('jsoneditor');
var $ = require('jquery');

var Form = B.Form;
var Col = B.Col;
var FormGroup = B.FormGroup;
var FormControl = B.FormControl;
var ControlLabel = B.ControlLabel;
var Button = B.Button;
var Select = require('../common/select');
var editor = null;

var FullPull = React.createClass({
    mixins: [Reflux.listenTo(store, "_onStatusChange")],
    getInitialState: function() {
        var state = store.initState();
        return state;
    },
    _onStatusChange: function(state) {
        this.setState(state);
        if(editor)
            editor.set(this.state.editor.json);
    },
    componentDidMount: function() {
        store.actions.initialLoad();
        this.initJsonEditor({});
    },
    initJsonEditor: function(value) {
        var container = this.refs.jsoneditor;
        var options = {
            sortObjectKeys: true,
            search: false,
            mode: 'view',
            modes: ['code', 'view']
        };
        editor = new JSONEditor(container, options);
        editor.set(value);
    },
    createDsList: function() {
        var list = [];
        this.state.dsOptions.forEach(function(ds, idx) {
            list.push(<option key={idx} value={ds.value} >{ds.text}</option>);
        });

        return list;
    },
    createSchemaList: function(dsParam) {
        store.actions.createSchemaList(dsParam);
    },
    createTableList: function(schemaParam) {
        store.actions.createTableList(schemaParam);
    },
    createTypeList:function(typeParam){
        store.actions.createTypeList(typeParam);
    },
    sendMessage: function(e) {
        var btn = $(e.target);
        btn.attr("disabled", true);

        var ctrlTopic = ReactDOM.findDOMNode(this.refs.ctrlTopic).value;
        var message = editor.get();

        var dsName = [];
        var dsId = ReactDOM.findDOMNode(this.refs.ds).value;
        this.state.dsOptions.forEach(function(ds, idx) {
            if(ds.value == dsId){
               var dsText = ds.text;
               dsName = dsText.split("/");
            }
        });
        var schemaId = ReactDOM.findDOMNode(this.refs.schema).value;
        var schemaName;
        this.state.schemaList.forEach(function(schema) {
            if(schema.value == schemaId){
               schemaName = schema.text;
            }
        });
        var tableName = ReactDOM.findDOMNode(this.refs.table).value;
        var tableOutputTopic;
        this.state.tableList.forEach(function(table) {
            if(table.value == tableName){
                tableOutputTopic = table.outputTopic;
            }
        });

        var outputTopic = ReactDOM.findDOMNode(this.refs.resultTopic).value;
        var strMessage = JSON.stringify(message);
        var param = {
            id: message.id,
            type: 'global',
            dsName: dsName[1],
            schemaName: schemaName,
            tableName: ReactDOM.findDOMNode(this.refs.table).value,
            ctrlTopic: ctrlTopic,
            outputTopic: outputTopic,
            tableOutputTopic: tableOutputTopic,
            message: strMessage
        };
        store.actions.sendMessage(param,function(msg) {
            btn.removeAttr('disabled');
            alert(msg);
        });
    },
    datasourceChanged:function(){
        var dsId = ReactDOM.findDOMNode(this.refs.ds).value;
        var dsParam = {dsId:dsId};
        this.createSchemaList(dsParam);
    },
    schemaChanged:function(){
        var dsId = ReactDOM.findDOMNode(this.refs.ds).value;
        
        //var schemaId = this.refs.schema.getValue();
        var schemaId = ReactDOM.findDOMNode(this.refs.schema).value;
        var schemaName;
        this.state.schemaList.forEach(function(schema, idx) {
            if(schema.value == schemaId){
               schemaName = schema.text;
            }
        });
        var schemaParam = {dsID:dsId,schemaName:schemaName};
        this.createTableList(schemaParam);
    },
    tableChanged:function(){
       var dsId = ReactDOM.findDOMNode(this.refs.ds).value;
       var schemaId = ReactDOM.findDOMNode(this.refs.schema).value;

       var dsName = [];
       this.state.dsOptions.forEach(function(ds, idx) {
            if(ds.value == dsId){
               var dsText = ds.text;
               dsName = dsText.split("/");
            }
        });
       var schemaName;
        this.state.schemaList.forEach(function(schema, idx) {
            if(schema.value == schemaId){
               schemaName = schema.text;
            }
        });
        var tableName = ReactDOM.findDOMNode(this.refs.table).value;
        var fullpullCol = "";
        var fullpullSplitShardSize = "";
        var fullpullSplitStyle = "";
        var physicalTables = "";
        this.state.tableList.forEach(function (e) {
            if(e.value == tableName) {
                physicalTables = e.physicalTables || "";
                fullpullCol = e.fullpullCol || "";
                fullpullSplitShardSize = e.fullpullSplitShardSize || "";
                fullpullSplitStyle = e.fullpullSplitStyle || "";
            }
        });

        var messageType = ReactDOM.findDOMNode(this.refs.messageType).value;
        //var resultTopic = ReactDOM.findDOMNode(this.refs.resultTopic).value;
         //初始化ds列表时赋值ctrl_topic
        ReactDOM.findDOMNode(this.refs.ctrlTopic).value = "global_ctrl_topic";
        var resultTopic = "global." + dsName[1] +"."+ schemaName + "." + tableName +".result";
        ReactDOM.findDOMNode(this.refs.resultTopic).value = resultTopic;

        var version = ReactDOM.findDOMNode(this.refs.version).checked;
        var batch = ReactDOM.findDOMNode(this.refs.batch).checked;
        this.createTypeList({
            dsId: dsId,
            schemaName: schemaName,
            tableName: tableName,
            resultTopic: resultTopic,
            physicalTables: physicalTables,
            version: version,
            batch: batch,
            messageType: messageType,
            fullpullCol: fullpullCol,
            fullpullSplitShardSize: fullpullSplitShardSize,
            fullpullSplitStyle: fullpullSplitStyle
        });
    },
    versionChanged:function(){
      var version = ReactDOM.findDOMNode(this.refs.version).checked;
      store.actions.setVersion(version);
    },
    batchChanged:function(){
      var batch = ReactDOM.findDOMNode(this.refs.batch).checked;
      store.actions.setBatch(batch);
    },
    resultTopicChanged:function(){
      var resultTopic = ReactDOM.findDOMNode(this.refs.resultTopic).value;
      store.actions.setResultTopic(resultTopic);
    },
    render: function() {
        return (
            <div className="container-fluid">
                <div className="row header">
                    <h4 className="col-xs-12">
                        Full Data Pull 
                    </h4>
                </div>
                <div className="row text-center">
                </div>
                <div className="row body">
                    <Form horizontal>
                        <FormGroup>
                            <Col componentClass={ControlLabel} sm={2}>
                                message type
                            </Col>
                            <Col sm={4}>
                                <FormControl
                                    ref="messageType"
                                    componentClass="input"
                                    value="全局独立全量拉取">
                                </FormControl>
                            </Col>
                        </FormGroup>

                         <div ref="verDiv" style={{display: ""}}>
                               <FormGroup>
                                 <Col componentClass={ControlLabel} sm={2}>
                                  increase_version
                                 </Col>
                                 <Col sm={4}>
                                   <label>
                                     <input type="checkbox"  ref="version"  onChange={this.versionChanged}></input>
                                   </label>
                                 </Col>
                        </FormGroup>
                        </div>

                         <div ref="batchDiv" style={{display: ""}}>
                               <FormGroup>
                                 <Col componentClass={ControlLabel} sm={2}>
                                  increase_batch_id
                                 </Col>
                                 <Col sm={4}>
                                   <label>
                                     <input type="checkbox"  ref="batch"  onChange={this.batchChanged}></input>
                                   </label>
                                 </Col>
                        </FormGroup>
                        </div>

                      <div ref="dsDiv" style={{display: ""}}>
                        <FormGroup>                            
                            <Col componentClass={ControlLabel} sm={2}>
                                data source
                            </Col>
                            <Col sm={4}>
                                
                                <FormControl
                                    ref="ds"
                                    onChange={this.datasourceChanged}
                                    componentClass="select">
                                    <option value="-1" >select data source</option>
                                    { this.createDsList() }
                                </FormControl>
                            </Col>          
                        </FormGroup>
                        </div>

                        <div ref="schemaDiv" style={{display: ""}}>
                        <FormGroup>                            
                            <Col componentClass={ControlLabel} sm={2}>
                                schema name
                            </Col>
                            <Col sm={4}>
                                
                                <FormControl
                                    ref="schema"
                                    onChange={this.schemaChanged}
                                    componentClass="select">
                                    <option value="-1" >select a schema</option>
                                    {this.state.schemaOptions}
                                </FormControl>
                            </Col>          
                        </FormGroup>
                        </div>

                        <div ref="tableDiv" style={{display: ""}}>
                        <FormGroup>                            
                            <Col componentClass={ControlLabel} sm={2}>
                                table name
                            </Col>
                            <Col sm={4}>
                             <FormControl
                                    ref="table"
                                    onChange={this.tableChanged}
                                    componentClass="select">
                                    <option value="-1" >select a table</option>
                                    {this.state.tableOptions}
                                </FormControl>      
                            </Col>          
                        </FormGroup>
                        </div>

                        <div ref="ctrlDiv" style={{display: ""}}>
                         <FormGroup>
                            <Col componentClass={ControlLabel} sm={2}>
                                ctrl_topic
                            </Col>
                            <Col sm={4}>
                                <FormControl
                                    ref="ctrlTopic"
                                    componentClass="input">
                                </FormControl>
                            </Col>
                        </FormGroup>
                        </div>

                        <div ref="resultDiv" style={{display: ""}}>
                         <FormGroup>
                            <Col componentClass={ControlLabel} sm={2}>
                                result_topic
                            </Col>
                            <Col sm={4}>
                                <FormControl
                                    ref="resultTopic"
                                    componentClass="input"
                                    onChange={this.resultTopicChanged}>
                                </FormControl>
                            </Col>
                        </FormGroup>
                        </div>
                       
                        <FormGroup>
                            <Col smOffset={2} sm={6}>
                                <div ref="jsoneditor" style={{height:210}}></div>
                            </Col>
                        </FormGroup>
                        <FormGroup>
                            <Col smOffset={2} sm={6}>
                                <Button type="button" bsStyle="primary" onClick={this.sendMessage}>
                                    <span className="glyphicon glyphicon-send"></span> Send Full Pull Request
                                </Button>
                            </Col>
                        </FormGroup>
                    </Form>
                </div>
            </div>
        );
    }
});

module.exports = FullPull;