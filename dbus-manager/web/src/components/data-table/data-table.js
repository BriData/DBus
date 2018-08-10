var React = require('react');
var ReactDOM = require('react-dom');
var Reflux = require('reflux');
var B = require('react-bootstrap');
var Tab = require('fixed-data-table');
var TF = require('../common/table/tab-frame');
var Select = require('../common/select');
var Table = require('../common/table/default-table');
var store = require('./data-table-store');
var cells = require('../common/table/cells');
var minxin = require('../common/table/mixin');
var utils = require('../common/utils');
var $ = require('jquery');
var antd = require('antd');

var Column = Tab.Column;
var Cell = Tab.Cell;
var TextCell = cells.TextCell;

var Modal = B.Modal;
var StatusCell = cells.StatusCell;
const {BtnCell} = require('../common/table/cells');

//var Table =  require('../dbus-data/dbus-data-sqltable');

var DataTable = React.createClass({
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
    },
    // 查询按钮响应事件
    search: function(e, pageNum) {
        var p = {
            dsID: this.refs.ds.getValue(),
            schemaName: this.refs.schema_name.value.trim(),
            tableName: this.refs.table_name.value.trim()
        };
        var param = buildQueryParmeter(p, pageNum);
        //console.log("param: " + JSON.stringify(param));
        store.actions.search(param);
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
    readOutputTopic: function (obj) {
        store.actions.readOutputTopic(obj);
    },
    closeReadOutputTopic: function () {
        store.actions.closeReadOutputTopic();
    },
    openDialogByKey: function(key, obj) {
        store.actions.openDialogByKey(key, obj);
    },
    openDialogConfigure: function(obj) {
        utils.showLoading();
        store.actions.openDialogConfigure(obj, this);
    },
    saveConfigure: function(){
        store.actions.saveConfigure();
    },
    closeConfigure: function(){
        store.actions.closeConfigure();
    },
    closeDialogZK:function(){
        store.actions.closeDialogZK();
    },
    openVersionDifference: function(obj,e) {
        utils.showLoading();
        store.actions.openVersionDifference(obj, this);
    },
    configRule: function(obj) {
        store.actions.configRule(obj, this);
    },
    takeEffect: function (obj) {
        if (!confirm("Are you sure to take effect ?")) return;
        store.actions.takeEffect(obj);
    },
    openUpdate:function(data,e){
        var updateParam = {
            id:data.id,
            tableName:data.tableName,
            tableNameAlias:data.tableNameAlias,
            description:data.description,
            physicalTableRegex:data.physicalTableRegex,
            outputBeforeUpdateFlg:data.outputBeforeUpdateFlg,
            status:data.status,
            fullpullCol:data.fullpullCol,
            fullpullSplitShardSize:data.fullpullSplitShardSize,
            fullpullSplitStyle:data.fullpullSplitStyle
        };
        store.actions.openUpdate(updateParam);
        this.props.history.pushState({passParam: updateParam}, "/data-table/table-update");
    },
    changeInactive:function(data,e){
        if(!confirm("Are you sure to inactivate this table ?")) return;
        var param = {
            id:data.id,
            status:"inactive"
        };
        store.actions.changeInactive(param);
    },
    confirmStatusChange: function (data, e) {
        if(confirm("Are you sure to clear the flag ?"))
        {
            store.actions.confirmStatusChange(data, e.currentTarget);
        }
    },
    independentPullWhole:function(data,e){
        var date = new Date();
        var newResultTopic = prompt("Please input output topic :", 'independent.' + data.outputTopic + '.' + date.getTime());
        if (newResultTopic == null) return;
        var typeParam = {
            date: date,
            dsId: data.dsID,
            schemaName: data.schemaName,
            tableName: data.tableName,
            ctrlTopic: data.ctrlTopic,
            tableOutputTopic: data.outputTopic,
            physicalTables: data.physicalTableRegex,
            outputTopic: newResultTopic,
            resultTopic: newResultTopic,
            version: false,
            batch: false,
            messageType: '单表独立全量拉取'
        };
        store.actions.independentPullWhole(typeParam, data);
    },
    pullWhole:function(data,e){
        if (!confirm("Are you sure to pull full ?")) return;
        var pullParam = {
            id:data.id,
            dsId:data.dsID,
            dsName:data.dsName,
            schemaName:data.schemaName,
            tableName:data.tableName,
            status:data.status,
            physicalTableRegex:data.physicalTableRegex,
            outputTopic:data.outputTopic,
            // 在拉全量时，传入后台会解析为''，为了避免toInt时出错，传入0
            version:data.version || 0, 
            namespace:data.namespace,
            type:"load-data"
        };
        store.actions.pullWhole(pullParam);
    },
    pullIncrement:function(data,e){
        if (!confirm("Are you sure to pull increment ?")) return;
        var pullParam = {
            id:data.id,
            dsId:data.dsID,
            dsName:data.dsName,
            schemaName:data.schemaName,
            tableName:data.tableName,
            status:data.status,
            physicalTableRegex:data.physicalTableRegex,
            outputTopic:data.outputTopic,
            version:data.version,
            namespace:data.namespace,
            createTime:data.createTime,
            type:"no-load-data"
        };
        store.actions.pullIncrement(pullParam);
    },
    stop:function(data,e){
        if (!confirm("Are you sure to stop ?")) return;
        var stopParam = {
            id:data.id
        };
        store.actions.stop(stopParam);
    },
    readTableVersion:function(data,e){
        var versionParam = {
            dsName:data.dsName,
            schemaName:data.schemaName,
            tableName:data.tableName
        };
        store.actions.readTableVersion(versionParam);
        //this.props.history.pushState({}, "/zk-manager");
    },
    versionChanged:function(data,e){
        var dataParam = {
            path:this.state.dialog.identityZK,
            version:data
        };
        store.actions.versionChanged(dataParam,this);
    },

    refreshZookeeperNode: function () {
        var version = ReactDOM.findDOMNode(this.refs.tableVersion).childNodes[0].value;
        this.versionChanged(version);
    },
    deleteTable: function(data) {
        if(!confirm("Are you sure to delete this table?")) return;
        var param = {
            tableId: data.id
        };
        store.actions.deleteTable(param, this.search);
    },
    statusStyle: function(data) {
        if (data.status == "ok" && data.verChangeNoticeFlg == 1) {
            return "warning"
        }
        else if (data.status == "ok") {
            return "success"
        }
        else if (data.status == "inactive") {
            return "default"
        }
        else if (data.status == "waiting") {
            return "info"
        }
        else if (data.status == "abort") {
            return "danger"
        }
    },

    handleEncodePackageChange : function(value, rowIndex) {
        store.actions.encodePackageChange(value, rowIndex)
    },

    render: function() {
        var rows = this.state.data.list || [];

        var isDsTypeLog = function(dsType) {
            return dsType != null && dsType.startsWith("log_");
        };

        var isDsTypeMysqlOracle = function(dsType) {
            return dsType == utils.dsType.oracle || dsType == utils.dsType.mysql;
        };

        return (
            <TF>
                <TF.Header title="DataTable">
                    <Select
                        ref="ds"
                        defaultOpt={{value:0, text:"select a data source"}}
                        options={this.state.dsOptions}
                        onChange={this.dsSelected}/>
                    <input
                        ref="schema_name"
                        type="text"
                        className="search"
                        placeholder="schemaName" />
                    <input
                        ref="table_name"
                        type="text"
                        className="search"
                        placeholder="tableName" />
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
                                <antd.Dropdown disabled={!isDsTypeMysqlOracle(rows[props.rowIndex].dsType)} overlay={
                                    <antd.Menu>
                                        <antd.Menu.Item>
                                            <div onClick={this.independentPullWhole.bind(this, rows[props.rowIndex])}>
                                                <span className="glyphicon glyphicon-share-alt"></span>
                                                <span>{" Custom FullPuller"}</span>
                                            </div>
                                        </antd.Menu.Item>
                                    </antd.Menu> }>
                                    <antd.Button type="primary" onClick={this.pullWhole.bind(this, rows[props.rowIndex])}>
                                        <span className="glyphicon glyphicon-share-alt"></span>{" FullPuller"}<antd.Icon type="down" />
                                    </antd.Button>
                                </antd.Dropdown>
                                <antd.Dropdown overlay={
                                    <antd.Menu>
                                        <antd.Menu.Item>
                                            <div onClick={this.pullIncrement.bind(this, rows[props.rowIndex])}>
                                                <span className="glyphicon glyphicon-play"></span>
                                                <span>{" IncrementPuller"}</span>
                                            </div>
                                        </antd.Menu.Item>
                                        <antd.Menu.Item>
                                            <div onClick={this.stop.bind(this, rows[props.rowIndex])}>
                                                <span className="glyphicon glyphicon-stop"></span>
                                                <span>{" Stop"}</span>
                                            </div>
                                        </antd.Menu.Item>

                                        <antd.Menu.Divider />

                                        <antd.Menu.Item disabled={!isDsTypeMysqlOracle(rows[props.rowIndex].dsType)}>
                                            <div onClick={isDsTypeMysqlOracle(rows[props.rowIndex].dsType) ? this.readTableVersion.bind(this, rows[props.rowIndex]): null}>
                                                <span className="glyphicon glyphicon-info-sign"></span>
                                                <span>{" ReadZK"}</span>
                                            </div>
                                        </antd.Menu.Item>
                                        <antd.Menu.Item disabled={!isDsTypeMysqlOracle(rows[props.rowIndex].dsType)}>
                                            <div onClick={isDsTypeMysqlOracle(rows[props.rowIndex].dsType) ? this.openDialogConfigure.bind(this, rows[props.rowIndex]) : null}>
                                                <span className="glyphicon glyphicon-lock"></span>
                                                <span>{" Encode"}</span>
                                            </div>
                                        </antd.Menu.Item>
                                        <antd.Menu.Item>
                                            <div onClick={this.openUpdate.bind(this, rows[props.rowIndex])}>
                                                <span className="glyphicon glyphicon-edit"></span>
                                                <span>{" Modify"}</span>
                                            </div>
                                        </antd.Menu.Item>
                                        <antd.Menu.Item>
                                            <div onClick={this.changeInactive.bind(this, rows[props.rowIndex])}>
                                                <span className="glyphicon glyphicon-edit"></span>
                                                <span>{" Inactivate"}</span>
                                            </div>
                                        </antd.Menu.Item>
                                        <antd.Menu.Item disabled={!isDsTypeLog(rows[props.rowIndex].dsType)}>
                                            <div onClick={isDsTypeLog(rows[props.rowIndex].dsType) ? this.configRule.bind(this, rows[props.rowIndex]): null}>
                                                <span className="glyphicon glyphicon-cog"></span>
                                                <span>{" Rules"}</span>
                                            </div>
                                        </antd.Menu.Item>
                                        <antd.Menu.Item>
                                            <div onClick={this.deleteTable.bind(this, rows[props.rowIndex])}>
                                                <span className="glyphicon glyphicon-remove"></span>
                                                <span>{" Delete"}</span>
                                            </div>
                                        </antd.Menu.Item>

                                        <antd.Menu.Divider />

                                        <antd.Menu.Item>
                                            <div onClick={this.takeEffect.bind(this, rows[props.rowIndex])}>
                                                <span className="glyphicon glyphicon-ok"></span>
                                                <span>{" Take Effect"}</span>
                                            </div>
                                        </antd.Menu.Item>
                                    </antd.Menu>}>
                                    <antd.Button>More<antd.Icon type="down" /></antd.Button>
                                </antd.Dropdown>
                            </div></div></div></div>
                            )}
                            width={185}/>
                        <Column
                            header={ <Cell> id </Cell> }
                            cell={ <TextCell data={rows} col="id"onDoubleClick={this.openDialogByKey.bind(this,"id")}/>}
                            width={50} />
                        <Column
                            header={ <Cell>ds</Cell> }
                            cell={ <TextCell data={rows} col="dsName" onDoubleClick={this.openDialogByKey.bind(this,"dsName")}/>}
                            width={100} />
                        <Column
                            header={ <Cell>schema</Cell> }
                            cell={ <TextCell data={rows} col="schemaName" onDoubleClick={this.openDialogByKey.bind(this,"schemaName")}/>}
                            width={120} />
                        <Column
                            header={ <Cell>tableName</Cell> }
                            cell={ <TextCell data={rows} col="tableName" onDoubleClick={this.openDialogByKey.bind(this,"tableName")}/>}
                            width={210} />
                        <Column
                            header={ <Cell>tableNameAlias</Cell> }
                            cell={ <TextCell data={rows} col="tableNameAlias" onDoubleClick={this.openDialogByKey.bind(this,"tableNameAlias")}/>}
                            width={210} />
                        <Column
                            header={ <Cell>status</Cell> }
                            cell={props => (
                            <Cell style={{textAlign:'center'}}>
                                <span className={"label label-" + this.statusStyle(rows[props.rowIndex])}>
                                    {
                                         rows[props.rowIndex].showStatusHyperlink
                                         ?
                                         <a style={{textDecoration:"underline"}} onClick={this.confirmStatusChange.bind(this,rows[props.rowIndex])}>{rows[props.rowIndex].status}</a>
                                         :
                                         rows[props.rowIndex].status
                                    }
                                </span>
                            </Cell>
                            )}
                            width={70}
                        />
                        <Column
                            header={ <Cell>version</Cell> }
                            cell={props => (
                            <div className="fixedDataTableCellLayout_wrap1 public_fixedDataTableCell_wrap1"><div className="fixedDataTableCellLayout_wrap2 public_fixedDataTableCell_wrap2"><div className="fixedDataTableCellLayout_wrap3 public_fixedDataTableCell_wrap3"><div className="public_fixedDataTableCell_cellContent">

                            <span onClick={this.openVersionDifference.bind(this, rows[props.rowIndex])} title="Click to View Version History" style={{fontWeight:"bold",textDecoration:"underline",cursor:"pointer"}}>
                                {rows[props.rowIndex]["version"]}
                            </span>
                            <span>
                                {(function(version, history){
                                    if(version == null || history == null) return null;
                                    return "<<"+history;
                                })(rows[props.rowIndex]["version"], rows[props.rowIndex]["versionsChangeHistory"])}
                            </span>
                            </div></div></div></div>
                            )
                            }
                            width={70} />
                        <Column
                            header={ <Cell>description</Cell> }
                            cell={ <TextCell data={rows} col="description" onDoubleClick={this.openDialogByKey.bind(this,"description")}/>}
                            width={150} />
                        <Column
                            header={ <Cell>createTime</Cell> }
                            cell={ <TextCell data={rows} col="createTime" onDoubleClick={this.openDialogByKey.bind(this,"createTime")}/>}
                            width={180} />
                        <Column
                            header={ <Cell>physicalTableRegex</Cell> }
                            cell={ <TextCell data={rows} col="physicalTableRegex"  ref="physicalTableRegex" onDoubleClick={this.openDialogByKey.bind(this,"physicalTableRegex")}/>}
                            width={200} />
                        <Column
                            header={ <Cell>outputTopic</Cell> }
                            cell={props => (
                            <div className="fixedDataTableCellLayout_wrap1 public_fixedDataTableCell_wrap1"><div className="fixedDataTableCellLayout_wrap2 public_fixedDataTableCell_wrap2"><div className="fixedDataTableCellLayout_wrap3 public_fixedDataTableCell_wrap3"><div className="public_fixedDataTableCell_cellContent">
                            <span onClick={this.readOutputTopic.bind(this, rows[props.rowIndex])} title="Read output topic" style={{fontWeight:"bold",textDecoration:"underline",cursor:"pointer"}}>
                                {rows[props.rowIndex]["outputTopic"]}
                            </span>
                            </div></div></div></div>
                            )
                            }
                            width={200} />
                        <Column
                            header={ <Cell>namespace</Cell> }
                            cell={ <TextCell data={rows} col="namespace" onDoubleClick={this.openDialogByKey.bind(this,"namespace")}/>}
                            width={400} />
                    </Table>
                    <div id="dialogHolder">
                        <Modal
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
                        <Modal
                            bsSize="large"
                            show={this.state.dialog.showZK}
                            onHide={this.closeDialogZK}>
                            <Modal.Header closeButton>
                                <Modal.Title>{this.state.dialog.identityZK}</Modal.Title>
                            </Modal.Header>
                            <Modal.Body>
                                <Select
                                    ref="tableVersion"
                                    defaultOpt={{value:-1,text:"select a zookeeper node"}}
                                    options={this.state.tableVersion}
                                    onChange={this.versionChanged}/>
                                <B.Button style={{marginLeft: "15px"}} bsSize="sm" onClick={this.refreshZookeeperNode}>Refresh
                                </B.Button>
                                <br/><br/>
                                <textarea className="form-control" ref="zkResult"  rows="18" cols="70"></textarea>
                            </Modal.Body>
                            <Modal.Footer>
                                <B.Button onClick={this.closeDialogZK}>Close</B.Button>
                            </Modal.Footer>
                        </Modal>
                        <Modal
                            backdrop='static'
                            bsSize="large"
                            show={this.state.dialog.showConfigure}
                            onHide={this.closeConfigure}>
                            <Modal.Header closeButton>
                                <Modal.Title>{this.state.dialog.identityConfigure}</Modal.Title>
                            </Modal.Header>
                            <Modal.Body>
                                {this.state.dialog.contentConfigure}
                            </Modal.Body>
                            <Modal.Footer>
                                <B.Button onClick={this.saveConfigure}>Save</B.Button>
                                <B.Button onClick={this.closeConfigure}>Close</B.Button>
                            </Modal.Footer>
                        </Modal>

                        <Modal
                            bsSize="large"
                            show={this.state.showReadOutputTopic}
                            onHide={this.closeReadOutputTopic}>
                            <Modal.Header closeButton>
                                <Modal.Title>
                                    <B.FormGroup>
                                        <B.Col componentClass={B.ControlLabel} sm={2}>
                                            Topic:{this.state.dialogOutputTopic}
                                        </B.Col>
                                    </B.FormGroup>
                                </Modal.Title>
                            </Modal.Header>
                            <Modal.Body>
                                
                            </Modal.Body>
                            <Modal.Footer>
                                <B.Button onClick={this.closeReadOutputTopic}>Close</B.Button>
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
    if(p.dsID != 0) {
        param.dsID = p.dsID;
    }
    if(p.schemaName != "") {
        param.schemaName = p.schemaName;
    }
    if(p.tableName != "") {
        param.tableName = p.tableName;
    }
    return param;
}

module.exports = DataTable;
