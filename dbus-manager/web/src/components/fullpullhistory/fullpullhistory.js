var React = require('react');
var ReactDOM = require('react-dom');
var $ =require('jquery');
var Reflux = require('reflux');
var B = require('react-bootstrap');
var Tab = require('fixed-data-table');
var TF = require('../common/table/tab-frame');
var Select = require('../common/select');
var Table = require('../common/table/default-table');
var store = require('./fullpullrecord-history');
var cells = require('../common/table/cells');
var antd = require('antd');
var Column = Tab.Column;
var Cell = Tab.Cell;
var TextCell = cells.TextCell;
var utils = require('../common/utils');

var Modal = B.Modal;
var StatusCell = cells.StatusCell;


var DataSource = React.createClass({
    mixins: [Reflux.listenTo(store, "_onStatusChange")],
    getInitialState: function() {
        var state = store.initState();
        return state;
    },
    componentDidMount: function() {
        utils.showLoading();
        this.search();
    },

    // 查询按钮响应事件
    search: function(e, pageNum) {
        var p = {
            pageNum: pageNum,
            dsName: ReactDOM.findDOMNode(this.refs.ds_name).value,
            schemaName: ReactDOM.findDOMNode(this.refs.schema_name).value,
            tableName: ReactDOM.findDOMNode(this.refs.table_name).value
        };
        p = buildQueryParmeter(p, pageNum);
        store.actions.search(p);
    },
    pageChange: function(e, pageNum) {
        this.search(e, pageNum);
    },
    _onStatusChange: function(state) {
        this.setState(state);
    },
    _closeDialog: function() {
        store.actions.closeDialog();
    },
    openDialogByKey: function(key, obj) {
        store.actions.openDialogByKey(key, obj);
    },
    
    render: function() {
        var rows = this.state.data.list || [];
        return (
            <TF>
                <TF.Header title="FullPull History">
                    <input
                        ref="ds_name"
                        type="text"
                        className="search"
                        placeholder="datasource name" />
                    <input
                        ref="schema_name"
                        type="text"
                        className="search"
                        placeholder="schema name" />
                    <input
                        ref="table_name"
                        type="text"
                        className="search"
                        placeholder="table name" />
                    <B.Button
                        bsSize="sm"
                        bsStyle="warning"
                        onClick={this.search}>
                        <span className="glyphicon glyphicon-search">
                        </span> Search
                    </B.Button>
                </TF.Header>
                <TF.Body pageCount={this.state.data.pages} onPageChange={this.pageChange}>
                    <Table rowsCount={rows.length} width={document.documentElement.clientWidth - 210}>
                        <Column
                            header={ <Cell > id </Cell> }
                            cell={ <TextCell data={rows}  col="id"  onDoubleClick={this.openDialogByKey.bind(this,"id")}/>}
                            width={115} />
                        <Column
                            header={ <Cell > type </Cell> }
                            cell={ <TextCell data={rows}  col="type"  onDoubleClick={this.openDialogByKey.bind(this,"type")}/>}
                            width={70} />
                        <Column
                            header={ <Cell > dsName </Cell> }
                            cell={ <TextCell data={rows}  col="dsName"  onDoubleClick={this.openDialogByKey.bind(this,"dsName")}/>}
                            width={70} />
                        <Column
                            header={ <Cell > schemaName </Cell> }
                            cell={ <TextCell data={rows}  col="schemaName"  onDoubleClick={this.openDialogByKey.bind(this,"schemaName")}/>}
                            width={160} />
                        <Column
                            header={ <Cell > tableName </Cell> }
                            cell={ <TextCell data={rows}  col="tableName"  onDoubleClick={this.openDialogByKey.bind(this,"tableName")}/>}
                            width={160} />
                        <Column
                            header={ <Cell > version </Cell> }
                            cell={ <TextCell data={rows}  col="version"  onDoubleClick={this.openDialogByKey.bind(this,"version")}/>}
                            width={70} />
                        <Column
                            header={ <Cell > batch_id </Cell> }
                            cell={ <TextCell data={rows}  col="batchId"  onDoubleClick={this.openDialogByKey.bind(this,"batchId")}/>}
                            width={70} />
                        <Column
                            header={ <Cell > state </Cell> }
                            cell={ <TextCell data={rows}  col="state"  onDoubleClick={this.openDialogByKey.bind(this,"state")}/>}
                            width={60} />
                        <Column
                            header={ <Cell > error_msg </Cell> }
                            cell={ <TextCell data={rows}  col="errorMsg"  onDoubleClick={this.openDialogByKey.bind(this,"errorMsg")}/>}
                            width={100} />
                        <Column
                            header={ <Cell > update_time </Cell> }
                            cell={ <TextCell data={rows}  col="updateTime"  onDoubleClick={this.openDialogByKey.bind(this,"updateTime")}/>}
                            width={180} />
                        <Column
                            header={ <Cell > init_time </Cell> }
                            cell={ <TextCell data={rows}  col="initTime"  onDoubleClick={this.openDialogByKey.bind(this,"initTime")}/>}
                            width={180} />
                        <Column
                            header={ <Cell > start_split_time </Cell> }
                            cell={ <TextCell data={rows}  col="startSplitTime"  onDoubleClick={this.openDialogByKey.bind(this,"startSplitTime")}/>}
                            width={180} />
                        <Column
                            header={ <Cell > start_pull_time </Cell> }
                            cell={ <TextCell data={rows}  col="startPullTime"  onDoubleClick={this.openDialogByKey.bind(this,"startPullTime")}/>}
                            width={180} />
                        <Column
                            header={ <Cell > end_time </Cell> }
                            cell={ <TextCell data={rows}  col="endTime"  onDoubleClick={this.openDialogByKey.bind(this,"endTime")}/>}
                            width={180} />

                        <Column
                            header={ <Cell > finished_partition_count </Cell> }
                            cell={ <TextCell data={rows}  col="finishedPartitionCount"  onDoubleClick={this.openDialogByKey.bind(this,"finishedPartitionCount")}/>}
                            width={180} />
                        <Column
                            header={ <Cell > total_partition_count </Cell> }
                            cell={ <TextCell data={rows}  col="totalPartitionCount"  onDoubleClick={this.openDialogByKey.bind(this,"totalPartitionCount")}/>}
                            width={180} />
                        <Column
                            header={ <Cell > finished_row_count </Cell> }
                            cell={ <TextCell data={rows}  col="finishedRowCount"  onDoubleClick={this.openDialogByKey.bind(this,"finishedRowCount")}/>}
                            width={180} />
                        <Column
                            header={ <Cell > total_row_count </Cell> }
                            cell={ <TextCell data={rows}  col="totalRowCount"  onDoubleClick={this.openDialogByKey.bind(this,"totalRowCount")}/>}
                            width={180}
                            flexGrow={1}/>
                    </Table>
                    <div id="dialogHolder">
                        <Modal
                            bsSize="large"
                            show={this.state.dialog.show}
                            onHide={this._closeDialog}>
                            <Modal.Header closeButton>
                                <Modal.Title>{this.state.dialog.identity}</Modal.Title>
                            </Modal.Header>
                            <Modal.Body>
                                <div dangerouslySetInnerHTML={{__html: "<div style='word-wrap: break-word'>"+this.state.dialog.content+"</div>"}} ></div>
                            </Modal.Body>
                            <Modal.Footer>
                                <B.Button onClick={this._closeDialog}>Close</B.Button>
                            </Modal.Footer>
                        </Modal>
                    </div>
                </TF.Body>

            </TF>
        );
    }
});

function buildQueryParmeter(p, pageNum) {
    var param = {
        pageSize:utils.getFixedDataTablePageSize(),
        pageNum: (typeof pageNum) == 'number'  ? pageNum : 1
    };

    if(p.dsName != "") {
        param.dsName = p.dsName;
    }
    if(p.schemaName != "") {
        param.schemaName = p.schemaName;
    }
    if(p.tableName != "") {
        param.tableName = p.tableName;
    }
    return param;
}

module.exports = DataSource;
