var React = require('react');
var Reflux = require('reflux');
var B = require('react-bootstrap');
var Tab = require('fixed-data-table');
var TF = require('../common/table/tab-frame');
var Select = require('../common/select');
var Table = require('../common/table/default-table');
var store = require('./step-2-log-processor/store');
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
    _onStatusChange: function (state) {
        this.setState(state);
    },
    getInitialState: function () {
        var state = store.initState();
        return state;
    },
    componentDidMount: function () {
        this.initSelect(store);
        var dsName = this.props.location.state.dsName;
        var schemaName = dsName + '_schema';
        var srcName = dsName + '.' + schemaName;
        var targetName =  srcName + '.result';
        this.refs.schema.value = schemaName;
        this.refs.src_topic.value = srcName;
        this.refs.target_topic.value = targetName;
    },
    schemaNameChange: function (event) {
        var dsName = this.props.location.state.dsName;
        var schemaName = this.refs.schema.value;
        var srcName = dsName + '.' + schemaName;
        var targetName = srcName + '.result';
        this.refs.src_topic.value = srcName;
        this.refs.target_topic.value= targetName;
    },
    nextStep: function () {
        if (this.state.tables.length == 0) {
            alert("请至少添加一张表!");
            return;
        }
        var passParam = this.props.location.state;
        var param = {
            schema: {
                dsId: passParam.dsId,
                dsName: passParam.dsName,
                schemaName: this.refs.schema.value,
                description: this.refs.description.value,
                status: this.refs.status.value,
                srcTopic: this.refs.src_topic.value,
                targetTopic: this.refs.target_topic.value
            },
            tables: this.state.tables
        };
        store.actions.nextStep(param, this.redirect);
    },
    redirect: function () {
        var passParam = this.props.location.state;
        this.props.history.pushState({dsType: passParam.dsType, dsName: passParam.dsName}, "/nds-step/nds-zkConf");
    },
    openDialog: function () {
        var targetName = this.refs.target_topic.value;
        store.actions.openDialog(targetName);
    },
    closeDialog: function () {
        store.actions.closeDialog();
    },
    okDialog: function () {
        var newTableName = this.refs.newTableName.value;
        if(utils.isEmpty(newTableName)) {
            alert("empty name!");
            return;
        }
        var newOutputTopic = this.refs.newOutputTopic.value;
        if(utils.isEmpty(newOutputTopic)) {
            alert("empty topic!");
            return;
        }
        var param = {
            tableName: newTableName,
            outputTopic: newOutputTopic,
            createTime: new Date().format("yyyy-MM-dd hh:mm:ss")
        };
        store.actions.okDialog(param);
    },
    inputDialogKeyDown: function (event) {
        if (event.keyCode == 13) this.okDialog();
    },
    render: function () {
        var passParam = this.props.location.state;
        var rows = this.state.tables || [];
        return (
            <div>
                <div className="row header">
                    <h4 className="col-xs-12">New DataLine</h4>
                    <div className="col-xs-9">
                        <StepCursor currentStep={1}/><br/>
                    </div>
                </div>
                <TF>
                    <TF.Body>
                        <div className="container-fluid"
                             style={{border:"solid 1px #ddd",borderRadius:"4px",padding:"15px",background:"#f7f7f9",marginBottom:"10px"}}>
                            <div className="row form">
                                <div className="col-xs-4">
                                    <div className="form-label">data source:</div>
                                    <div className="control">
                                        <input ref="data_source" type="text" className="form-control control-height"
                                               disabled value={passParam.dsName+'/'+passParam.dsType}/>
                                    </div>
                                </div>
                                <div className="col-xs-4">
                                    <div className="form-label">schema:</div>
                                    <div className="control">
                                        <input ref="schema" type="text" className="form-control control-height"
                                               onChange={this.schemaNameChange}/>
                                    </div>
                                </div>
                                <div className="col-xs-4">
                                    <div className="form-label">description:</div>
                                    <div className="control">
                                        <input ref="description" type="text" className="form-control control-height"/>
                                    </div>
                                </div>
                            </div>

                            <div className="row form">
                                <div className="col-xs-4">
                                    <div className="form-label">status:</div>
                                    <div className="control">
                                        <input ref="status" type="text" className="form-control control-height"
                                               defaultValue="active"/>
                                    </div>
                                </div>
                                <div className="col-xs-4">
                                    <div className="form-label">src_topic:</div>
                                    <div className="control">
                                        <input ref="src_topic" type="text" className="form-control control-height"/>
                                    </div>
                                </div>
                                <div className="col-xs-4">
                                    <div className="form-label">target_topic:</div>
                                    <div className="control">
                                        <input ref="target_topic" type="text" className="form-control control-height"/>
                                    </div>
                                </div>
                            </div>

                            <div className="row">
                                <div className="col-xs-12 text-right">
                                    <B.Button className="btn-info" bsStyle="info" onClick={this.openDialog}>
                                        Add Table
                                    </B.Button>
                                    <span className="btn-span"></span>
                                    <B.Button className="btn-info" bsStyle="warning" onClick={this.nextStep}>
                                        Next
                                    </B.Button>
                                </div>
                            </div>
                        </div>

                        <Table rowsCount={rows.length}>
                            <Column header={<Cell> Table Name </Cell>}
                                    cell={< TextCell data = { rows }
                                    col = "tableName" />}
                                    width={400}/>
                            <Column header={<Cell> Output Topic </Cell>}
                                    cell={< TextCell data = { rows }
                                    col = "outputTopic" />}
                                    width={450}/>
                            <Column header={<Cell> Create Time </Cell>}
                                    cell={< TextCell data = { rows }
                                    col = "createTime" />}
                                    width={450}
                                    flexGrow={1}/>
                        </Table>
                    </TF.Body>
                </TF>

                <Modal bsSize="lg" show={this.state.dialog.show} onHide={this.closeDialog}>
                    <Modal.Header closeButton>
                        <Modal.Title><h4>Add Table</h4></Modal.Title>
                    </Modal.Header>
                    <Modal.Body>
                        <div className="form-label">Table Name:</div>
                        <div className="control">
                            <input ref="newTableName" type="text" className="form-control control-height" onKeyDown={this.inputDialogKeyDown}/>
                        </div>
                        <div className="form-label">Output Topic:</div>
                        <div className="control">
                            <input ref="newOutputTopic" type="text" className="form-control control-height" onKeyDown={this.inputDialogKeyDown}
                            defaultValue={this.state.newOutputTopic}/>
                        </div>
                    </Modal.Body>
                    <Modal.Footer>
                        <B.Button onClick={this.okDialog}>OK</B.Button>
                        <B.Button onClick={this.closeDialog}>Close</B.Button>
                    </Modal.Footer>
                </Modal>
            </div>
        );
    }
});

module.exports = StepSecond;
