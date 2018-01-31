var React = require('react');
var ReactDOM = require('react-dom');
var $ =require('jquery');
var Reflux = require('reflux');
var B = require('react-bootstrap');
var Tab = require('fixed-data-table');
var TF = require('../common/table/tab-frame');
var Select = require('../common/select');
var Table = require('../common/table/default-table');
var store = require('./datasource-store');
var cells = require('../common/table/cells');
var Link = require('react-router').Link;
var antd = require('antd');
var Column = Tab.Column;
var Cell = Tab.Cell;
var TextCell = cells.TextCell;
var CheckboxCell = cells.CheckboxCell;
var utils = require('../common/utils');

var Modal = B.Modal;
var StatusCell = cells.StatusCell;
var BtnCell = cells.BtnCell;

//var Table =  require('../dbus-data/dbus-data-sqltable');

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
    _closeDialog: function() {
        store.actions.closeDialog();
    },
    openDialogByKey: function(key, obj) {
        store.actions.openDialogByKey(key, obj);
    },
    openUpdate:function(data,e){
        var updateParam = {
            id:data.id,
            dsName:data.dsName,
            dsType:data.dsType,
            user:data.dbusUser,
            password:data.dbusPassword,
            desc:data.dsDesc,
            masterURL:data.masterURL,
            slaveURL:data.slaveURL,
            dsPartition:data.dsPartition
        };
        this.props.history.pushState({passParam: updateParam}, "/datasource/ds-update");
    },
    createSchema:function(data,e){
        this.props.history.pushState({dsId:data.id,dsType:data.dsType,dsName:data.dsName}, "/modify/schema-table");
    },
    deleteDataSource:function(data,e){
        if(!confirm("Are you sure to delete this datasource?")) return;
        var param = {
            dsId: data.id
        };
        store.actions.deleteDataSource(param, this.search);
    },
    render: function() {
        var rows = this.state.data.list || [];

        var isDsTypeMysqlOracle = function(dsType) {
            return dsType == utils.dsType.oracle || dsType == utils.dsType.mysql;
        };

        return (
            <TF>
                <TF.Header title="DataSource ">
                    <input
                        ref="text"
                        type="text"
                        className="search"
                        placeholder="data source name" />
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
                                <antd.Button disabled={!isDsTypeMysqlOracle(rows[props.rowIndex].dsType)}
                                    onClick={this.createSchema.bind(this, rows[props.rowIndex])}>
                                    <span className="glyphicon glyphicon-plus"></span>
                                    {" add schema"}
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
                                            <div onClick={this.deleteDataSource.bind(this, rows[props.rowIndex])}>
                                                <span className="glyphicon glyphicon-remove"></span>
                                                <span>{" delete"}</span>
                                            </div>
                                        </antd.Menu.Item>
                                    </antd.Menu>}>
                                    <antd.Button>More<antd.Icon type="down" /></antd.Button>
                                </antd.Dropdown>
                            </div></div></div></div>
                            )}
                            width={180}/>
                        <Column
                            header={ <Cell > id </Cell> }
                            cell={ <TextCell data={rows}  col="id"  onDoubleClick={this.openDialogByKey.bind(this,"id")}/>}
                            width={50} />
                        <Column
                            header={<Cell> dsName  </Cell> }
                            cell={ <TextCell data={rows} col="dsName" onDoubleClick={this.openDialogByKey.bind(this,"dsName")}/> }
                            width={150} />
                        <Column
                            header={ <Cell> dsType  </Cell> }
                            cell={ <TextCell data={rows} col="dsType" onDoubleClick={this.openDialogByKey.bind(this,"dsType")}/>}
                            width={100} />
                        <Column
                            header={ <Cell> status </Cell> }
                            cell={ <StatusCell style={{textAlign:'center'}} data={rows}  styleGetter={function(data) {return data.status == "active" ? "success": "default"}} col="status" />}
                            width={70}/>
                        <Column
                            header={ <Cell> dsDesc </Cell> }
                            cell={ <TextCell data={rows} col="dsDesc"  onDoubleClick={this.openDialogByKey.bind(this,"dsDesc")}/>}
                            width={200} />
                        <Column
                            header={ <Cell> updateTime </Cell> }
                            cell={ <TextCell data={rows} col="updateTime" onDoubleClick={this.openDialogByKey.bind(this,"updateTime")}/>}
                            width={180}/>
                        <Column
                            header={<Cell> topic  </Cell> }
                            cell={ <TextCell data={rows} col="topic" onDoubleClick={this.openDialogByKey.bind(this,"topic")}/> }
                            width={150} />
                        <Column
                            header={<Cell> ctrlTopic  </Cell> }
                            cell={ <TextCell data={rows} col="ctrlTopic" onDoubleClick={this.openDialogByKey.bind(this,"ctrlTopic")}/> }
                            width={150} />
                        <Column
                            header={<Cell> schemaTopic  </Cell> }
                            cell={ <TextCell data={rows} col="schemaTopic" onDoubleClick={this.openDialogByKey.bind(this,"schemaTopic")}/> }
                            width={150} />
                        <Column
                            header={<Cell> splitTopic  </Cell> }
                            cell={ <TextCell data={rows} col="splitTopic" onDoubleClick={this.openDialogByKey.bind(this,"splitTopic")}/> }
                            width={150} />
                        <Column
                            header={ <Cell> dbusUser </Cell> }
                            cell={ <TextCell data={rows} col="dbusUser" onDoubleClick={this.openDialogByKey.bind(this,"dbusUser")}/>}
                            width={100} />
                        <Column
                            header={ <Cell> masterURL </Cell> }
                            cell={ <TextCell data={rows} col="masterURL"  onDoubleClick={this.openDialogByKey.bind(this,"masterURL")}/>}
                            width={500} />
                        <Column
                            header={ <Cell> slaveURL </Cell> }
                            cell={ <TextCell data={rows} col="slaveURL" onDoubleClick={this.openDialogByKey.bind(this,"slaveURL")}/>}
                            width={500} />
                        <Column
                            header={ <Cell> dsPartition </Cell> }
                            cell={ <TextCell data={rows} col="dsPartition" onDoubleClick={this.openDialogByKey.bind(this,"dsPartition")}/>}
                            width={500}
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

    if(p.text != "") {
        param.text = p.text;
    }
    return param;
}

module.exports = DataSource;
