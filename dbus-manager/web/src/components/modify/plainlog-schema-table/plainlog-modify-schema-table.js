var React = require('react');
var Reflux = require('reflux');
var B = require('react-bootstrap');
var Tab = require('fixed-data-table');
var TF = require('../../common/table/tab-frame');
var Select = require('../../common/select');
var Table = require('../../common/table/default-table');
var store = require('./plainlog-modify-schema-table-store');

var utils = require('../../common/utils');
var cells = require('../../common/table/cells');
var minxin = require('../../common/table/mixin');
var ReactDOM = require('react-dom');

var Column = Tab.Column;
var Cell = Tab.Cell;
var TextCell = cells.TextCell;
var CheckboxCell = cells.CheckboxCell;
var Modal = B.Modal;

var ModifySchemaTable = React.createClass({
    mixins: [
        Reflux.listenTo(store, "_onStatusChange"),
        minxin
    ],
    _onStatusChange: function (state) {
        this.setState(state);
    },
    getInitialState: function () {
        var state = store.initState();
        return state;
    },
    componentDidMount: function () {
        store.actions.initialLoad(this.props.location.state);
    },
    openDialog: function () {
        store.actions.openDialog();
    },
    closeDialog: function () {
        store.actions.closeDialog();
    },
    okDialog: function () {
        var passParam = this.props.location.state;
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
            dsId: passParam.dsId,
            dsName: passParam.dsName,
            schemaId: passParam.schemaId,
            schemaName: passParam.schemaName,
            tableName: newTableName,
            physicalTableRegex: newTableName,
            outputTopic: newOutputTopic,
            status: 'abort',
            ver_change_notice_flg: 0,
            output_before_update_flg: 0
        };
        store.actions.okDialog(param);
    },
    inputDialogKeyDown: function (event) {
        if(event.keyCode == 13) this.okDialog();
    },
    render: function () {
        var passParam = this.props.location.state;
        var rows = this.state.tables || [];
        return (
            <div>
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
                                        <input ref="schema" type="text" className="form-control control-height" disabled
                                               value={passParam.schemaName}/>
                                    </div>
                                </div>
                                <div className="col-xs-4">
                                    <div className="form-label">description:</div>
                                    <div className="control">
                                        <input ref="description" type="text" className="form-control control-height"
                                               disabled value={passParam.description}/>
                                    </div>
                                </div>
                            </div>

                            <div className="row form">
                                <div className="col-xs-4">
                                    <div className="form-label">status:</div>
                                    <div className="control">
                                        <input ref="status" type="text" className="form-control control-height" disabled
                                               value={passParam.status}/>
                                    </div>
                                </div>
                                <div className="col-xs-4">
                                    <div className="form-label">src_topic:</div>
                                    <div className="control">
                                        <input ref="src_topic" type="text" className="form-control control-height"
                                               disabled value={passParam.srcTopic}/>
                                    </div>
                                </div>
                                <div className="col-xs-4">
                                    <div className="form-label">target_topic:</div>
                                    <div className="control">
                                        <input ref="target_topic" type="text" className="form-control control-height"
                                               disabled value={passParam.targetTopic}/>
                                    </div>
                                </div>
                            </div>

                            <div className="row">
                                <div className="col-xs-12 text-right">
                                    <B.Button className="btn-info" bsStyle="info" onClick={this.openDialog}>
                                        Add Table
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
                                    width={350}
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
                            <input ref="newOutputTopic" type="text" className="form-control control-height" onKeyDown={this.inputDialogKeyDown} defaultValue={passParam.targetTopic}/>
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

module.exports = ModifySchemaTable;
