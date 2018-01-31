var React = require('react');
var Reflux = require('reflux');
var B = require('react-bootstrap');
var Tab = require('fixed-data-table');
var TF = require('../common/table/tab-frame');
var Select = require('../common/select');
var Table = require('../common/table/default-table');
var store = require('./step-2/store');
var StepCursor = require('./step-cursor');
var utils = require('../common/utils');
var cells = require('../common/table/cells');
var minxin = require('../common/table/mixin');
var ReactDOM = require('react-dom');

var Column = Tab.Column;
var Cell = Tab.Cell;
var TextCell = cells.TextCell;
var LinkCell = cells.LinkCell;
var CheckboxCell = cells.CheckboxCell;
var Modal = B.Modal;

var StepSecond = React.createClass({
    mixins: [Reflux.listenTo(store, "_onStatusChange"), minxin],
    getInitialState: function() {
        var state = store.initState();
        return state;
    },
    componentDidMount: function() {
        this.initSelect(store);
        store.actions.initialLoad();
        utils.showLoading();
        this.passParam();
    },
    _onStatusChange: function(state) {
        this.setState(state);
    },
    // 选择数据源后触发加载schema事件
    dsSelected: function(dsName) {
        store.actions.dataSourceSelected(dsName);
    },
    // 选择dataSchema后触发加载status事件
    schemaSelected: function(schemaName) {
        //utils.showLoading();
        var dsId = this.refs.ds.getValue();
        var schemaName = this.refs.schema.getValue();
        store.actions.dataSchemaSelected(dsId,schemaName,this.showDescription);
    },
    fillAndClearTable: function(isChecked,data) {
        var tableName = data.tableName;
        if(isChecked)
        {
            store.actions.fillTable(tableName);
        }
        else
        {
            store.actions.clearTable(tableName);
        }
    },
    showDescription: function(description){
        ReactDOM.findDOMNode(this.refs.description).value = description;
    },
    openDialog:function(){
        var schemaName = this.refs.schema.getValue();
        if(schemaName != 0)
        {
            store.actions.openDialog(schemaName);
        }
    },
    closeDialog: function() {
        store.actions.closeDialog();
    },
    //下一步
    nextStep: function(dsId,schemaName) {
        var dsId = this.refs.ds.getValue();
        var schemaName = this.refs.schema.getValue();
        var description = ReactDOM.findDOMNode(this.refs.description).value;
        var list = this.state.dsOptions;
        var start = list[0].text.indexOf("/");
        var dsName = list[0].text.substring(start + 1);
        store.actions.nextStep(dsId,schemaName,description,this.redirect);
    },
    redirect:function(flag,tablesCount){
        if(flag == 1){
            var list = this.state.dsOptions;
            var slashIndex = list[0].text.indexOf("/");
            var dsType = list[0].text.substring(0,slashIndex);
            var dsName = list[0].text.substring(slashIndex + 1);
            alert("Insert " + tablesCount + " tables success!");
            this.props.history.pushState({dsType:dsType,dsName:dsName}, "/nds-step/nds-zkConf");
        }
    },
    passParam:function(){
        var params = this.props.location.state;
        store.actions.passParam(params,this.showDescription);
    },
    render: function() {
        var rows = this.state.tableCol || [];
        var dsOptions = this.state.dsOptions[0] || [];
        var defaultSchemOpts = this.state.defaultSchemOpts[0] || [];
        if(defaultSchemOpts == '') {
            defaultSchemOpts.push({value:0, text:"select a schema"});
            defaultSchemOpts = defaultSchemOpts[0] || [];
        }
        var dsType = this.state.dsType || "";
        var display = "scriptShow";
        if(dsType != "oracle") {
            display = "scriptHide";
        }
        return (
            <div className="container-fluid">
                <div className="row header">
                    <h4 className="col-xs-12">New DataLine</h4>
                    <div className="col-xs-9">
                        <StepCursor currentStep={1}/><br/>
                    </div>
                </div>
                <div>
                    <TF>
                    <TF.Body pageCount={this.state.data.pages} onPageChange={this.pageChange}>
                        <div className="container-fluid" style={{border:"solid 1px #ddd",borderRadius:"4px",padding:"15px",background:"#f7f7f9",marginBottom:"10px"}}>
                            <div className="row form ">
                                <div className="col-xs-4">
                                    <div className="form-label">data source:</div>
                                    <div className="control">
                                        <Select ref="ds" defaultOpt={dsOptions} onChange={this.dsSelected}/>
                                    </div>
                                </div>
                                <div className="col-xs-4">
                                    <div className="form-label">schema:</div>
                                    <div className="control">
                                        <Select ref="schema" defaultOpt={defaultSchemOpts} options={this.state.schemaOpts} onChange={this.schemaSelected}/>
                                    </div>
                                </div>
                                <div className="col-xs-4">
                                    <div className="form-label">description:</div>
                                    <div className="control">
                                        <input ref="description" type="text" className="form-control control-height"/>
                                    </div>
                                </div>
                            </div>

                            <div className="row form row-height">
                                <div className="col-xs-4">
                                    <div className="form-label">status:</div>
                                    <div className="control">
                                        <input ref="status" type="text" className="form-control control-height" value={this.state.status}/>
                                    </div>
                                </div>
                                <div className="col-xs-4">
                                    <div className="form-label">src_topic:</div>
                                    <div className="control">
                                        <input ref="src_topic" type="text" className="form-control control-height" readonly="readonly" value={this.state.src_topic}/>
                                    </div>
                                </div>
                                <div className="col-xs-4">
                                    <div className="form-label">target_topic:</div>
                                    <div className="control">
                                        <input ref="target_topic" type="text" className="form-control control-height" readonly="readonly" value={this.state.target_topic}/>
                                    </div>
                                </div>
                            </div>

                            <div className="row">
                                <div className="col-xs-12 text-right">
                                    <B.Button bsSize={display} className="btn-info" bsStyle="info" onClick={this.openDialog}>
                                        Config Script
                                    </B.Button>
                                    <span className="btn-span"></span>
                                    <B.Button bsSize="next" className="btn-info" bsStyle="warning" onClick={this.nextStep}>
                                        Next
                                    </B.Button>
                                </div>
                            </div>
                        </div>

                        <Table rowsCount={rows.length}>
                            <Column header={<Cell data = { rows }> Table Name </Cell>}
                                    cell={< CheckboxCell data = { rows } emitter = { this.emitter }
                                col = "tableName" onChange = { this.fillAndClearTable } />}
                                    width={200}/>
                            <Column header={<Cell> Physical_table regex </Cell>}
                                    cell={< TextCell data = { rows }
                                col = "physicalTableRegex" />}
                                    width={250}/>
                            <Column header={<Cell> Output Topic </Cell>}
                                    cell={< TextCell data = { rows }
                                col = "outputTopic" />}
                                    width={250}/>
                            <Column header={<Cell> Incompatible Column </Cell>}
                                    cell={<TextCell data = {rows}
                                col = "incompatibleColumn" />}
                                    width={200}/>
                            <Column header={<Cell> DBUS Ignore Column </Cell>}
                                    cell={< TextCell data = { rows }
                                col = "columnName" />}
                                    width={400}
                                    flexGrow={1}/>
                        </Table>
                    </TF.Body>
                    </TF>
                <Modal
                    bsSize="lg"
                    show={this.state.dialog.show}
                    onHide={this.closeDialog}>
                    <Modal.Header closeButton>
                        <Modal.Title><h3>Config Script</h3></Modal.Title>
                    </Modal.Header>
                    <Modal.Body>
                      <div dangerouslySetInnerHTML={{__html: this.state.dialog.content}} ></div>
                    </Modal.Body>
                    <Modal.Footer>
                      <B.Button onClick={this.closeDialog}>Close</B.Button>
                    </Modal.Footer>
              </Modal>
                </div>
            </div>
        );
    }
});

module.exports = StepSecond;
