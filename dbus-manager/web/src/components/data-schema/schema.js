var React = require('react');
var Reflux = require('reflux');
var B = require('react-bootstrap');
var Tab = require('fixed-data-table');
var TF = require('../common/table/tab-frame');
var Select = require('../common/select');
var Table = require('../common/table/default-table');
var store = require('./schema-store');
var cells = require('../common/table/cells');
var minxin = require('../common/table/mixin');
var utils = require('../common/utils');
var antd = require("antd");
var Modal = B.Modal;
var Column = Tab.Column;
var Cell = Tab.Cell;
var TextCell = cells.TextCell;
var LinkCell = cells.LinkCell;
var CheckboxCell = cells.CheckboxCell;
var StatusCell = cells.StatusCell;
var BtnCell = cells.BtnCell;

//var Table =  require('../dbus-data/dbus-data-sqltable');

var Schema = React.createClass({
    mixins: [Reflux.listenTo(store, "_onStatusChange"), minxin],
    getInitialState: function() {
        var state = store.initState();
        return state;
    },
    componentDidMount: function() {
        // 监听select-all事件，在CheckboxCell中会发出select-all事件
        this.initSelect(store);
        utils.showLoading();
        store.actions.initialLoad();
        this.search();
    },

    // 选择数据源后触发加载schema事件
    // dsSelected: function(dsId) {
    //     store.actions.dataSourceSelected(dsId);
    // },
    // 查询按钮响应事件
    search: function(e, pageNum) {
        var p = {
            dsId: this.refs.ds.getValue(),
            text: this.refs.text.value.trim()
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
    /*
     _start:function(data, e) {
     //alert("start");
     var startParam = {
     id:data.id,
     status:"active"
     };
     store.actions.start(startParam);
     },
     _stop:function(data, e) {
     //alert("stop");
     var stopParam ={
     id:data.id,
     status:"inactive"
     };
     store.actions.stop(stopParam);
     },
     */
    createTable:function(data,e){
        if (data.dsType == utils.dsType.logLogstash 
            || data.dsType == utils.dsType.logLogstashJson 
            || data.dsType == utils.dsType.logUms
            || data.dsType == utils.dsType.logFlume
            || data.dsType == utils.dsType.logFilebeat) this.props.history.pushState({
            dsId: data.dsId,
            dsType: data.dsType,
            dsName: data.dsName,
            schemaId: data.id,
            schemaName: data.schemaName,
            srcTopic: data.srcTopic,
            targetTopic: data.targetTopic,
            description: data.description,
            status: data.status
        }, "/data-schema/modify/plainlog-schema-table");
        else if (data.dsType == utils.dsType.mysql
            || data.dsType == utils.dsType.oracle
            || data.dsType == utils.dsType.mongo) this.props.history.pushState({
            dsId: data.dsId,
            dsType: data.dsType,
            dsName: data.dsName,
            schemaName: data.schemaName
        }, "/data-schema/modify/schema-table");
    },
    _closeDialog: function() {
        store.actions.closeDialog();
    },
    openDialogByKey: function(key, obj) {
        store.actions.openDialogByKey(key, obj);
    },
    openUpdate:function(data,e){
        var updateParam = {
            dsId:data.dsId,
            schemaName:data.schemaName,
            description:data.description
        };
        this.props.history.pushState({passParam: updateParam}, "/data-schema/schema-update");
    },
    deleteSchema: function(data) {
        if(!confirm("Are you sure to delete this schema?")) return;
        var param = {
            schemaId: data.id
        };
        store.actions.deleteSchema(param, this.search);
    },
    render: function() {
        var rows = this.state.data.list || [];
        return (
            <TF>
                <TF.Header title="DataSchema">
                    <Select
                        ref="ds"
                        defaultOpt={{value:0, text:"select a data source"}}
                        options={this.state.dsOptions}
                        onChange={this.dsSelected}/>
                    <input
                        ref="text"
                        type="text"
                        className="search"
                        placeholder="schemaName" />
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
                            header={ <Cell>Operation</Cell> }
                            cell={ props => (
                            <div className="fixedDataTableCellLayout_wrap1 public_fixedDataTableCell_wrap1"><div className="fixedDataTableCellLayout_wrap2 public_fixedDataTableCell_wrap2"><div className="fixedDataTableCellLayout_wrap3 public_fixedDataTableCell_wrap3"><div className="public_fixedDataTableCell_cellContent btn-cell">
                                <antd.Button onClick={this.createTable.bind(this, rows[props.rowIndex])}>
                                    <span className="glyphicon glyphicon-plus"></span>
                                    {" add table"}
                                </antd.Button>
                                <antd.Dropdown overlay={
                                    <antd.Menu>
                                        <antd.Menu.Item>
                                            <div onClick={this.openUpdate.bind(this, rows[props.rowIndex])}>
                                                <span className="glyphicon glyphicon-edit"></span>
                                                <span>{" modify"}</span>
                                            </div>
                                        </antd.Menu.Item>
                                        <antd.Menu.Item>
                                            <div onClick={this.deleteSchema.bind(this, rows[props.rowIndex])}>
                                                <span className="glyphicon glyphicon-remove"></span>
                                                <span>{" delete"}</span>
                                            </div>
                                        </antd.Menu.Item>
                                    </antd.Menu>}>
                                    <antd.Button>More<antd.Icon type="down" /></antd.Button>
                                </antd.Dropdown>
                            </div></div></div></div>
                            )}
                            width={165}/>
                        <Column
                            header={ <Cell> id </Cell> }
                            cell={ <TextCell data={rows} col="id" onDoubleClick={this.openDialogByKey.bind(this,"id")}/>}
                            width={50} />
                        <Column
                            header={ <Cell>dsName</Cell> }
                            cell={ <TextCell data={rows} col="dsName" onDoubleClick={this.openDialogByKey.bind(this,"dsName")}/>}
                            width={150} />
                        <Column
                            header={ <Cell>schemaName</Cell> }
                            cell={ <TextCell data={rows} col="schemaName" onDoubleClick={this.openDialogByKey.bind(this,"schemaName")}/>}
                            width={150} />
                        <Column
                            header={ <Cell>status</Cell> }
                            cell={ <StatusCell style={{textAlign:'center'}} data={rows} styleGetter={function(data) {return data.status == "active" ? "success": "default"}} col="status" />}
                            width={70}
                        />
                        <Column
                            header={ <Cell>description</Cell> }
                            cell={ <TextCell data={rows} col="description" onDoubleClick={this.openDialogByKey.bind(this,"description")}/>}
                            width={200} />
                        <Column
                            header={ <Cell>updateTime</Cell> }
                            cell={ <TextCell data={rows} col="createTime" onDoubleClick={this.openDialogByKey.bind(this,"createTime")}/>}
                            width={180}
                        />
                        <Column
                            header={ <Cell>srcTopic</Cell> }
                            cell={ <TextCell data={rows} col="srcTopic" onDoubleClick={this.openDialogByKey.bind(this,"srcTopic")}/>}
                            width={250} />
                        <Column
                            header={ <Cell>targetTopic</Cell> }
                            cell={ <TextCell data={rows} col="targetTopic" onDoubleClick={this.openDialogByKey.bind(this,"targetTopic")}/>}
                            width={300}
                            flexGrow={1} />
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

    if(p.dsId != 0) {
        param.dsId = p.dsId;
    }

    if(p.text != "") {
        param.text = p.text;
    }
    return param;
}

module.exports = Schema;
