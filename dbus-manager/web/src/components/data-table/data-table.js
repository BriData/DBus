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
    openDialogByKey: function(key, obj) {
        store.actions.openDialogByKey(key, obj);
    },
    openDialogConfigure: function(obj) {
        utils.showLoading();
        store.actions.openDialogConfigure(obj);
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
    addRule:function(obj) {
        utils.showLoading();
        store.actions.openAddRule(obj, this);
    },
    openUpdate:function(data,e){
        var updateParam = {
            id:data.id,
            tableName:data.tableName,
            physicalTableRegex:data.physicalTableRegex,
            outputBeforeUpdateFlg:data.outputBeforeUpdateFlg,
            status:data.status
        };
        store.actions.openUpdate(updateParam);
        this.props.history.pushState({passParam: updateParam}, "/data-table/table-update");
    },
    confirmStatusChange: function (data, e) {
        if(confirm("Are you sure to clear the flag ?"))
        {
            store.actions.confirmStatusChange(data, e.currentTarget);
        }
    },
    pullWhole:function(data,e){
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
            type:"load-data"
        };
        store.actions.pullWhole(pullParam);
    },
    pullIncrement:function(data,e){
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
    render: function() {
        var rows = this.state.data.list || [];
        var btns = [
            {text:"IncrementPuller", bsStyle:"info", icon:"play", action:this.pullIncrement},
            {text:"FullPuller", bsStyle:"primary", icon:"share-alt", action:this.pullWhole},
            {text:"Stop", bsStyle:"warning", icon:"stop", action:this.stop},
            {text:"ReadZK", bsStyle:"success", icon:"info-sign", action:this.readTableVersion},
            {text:"Encode", bsStyle:"success", icon:"lock", action:this.openDialogConfigure},
            {text:"Modify", bsStyle:"default", icon:"edit", action:this.openUpdate}];
        if(global.isDebug) {
            btns.push({text:"add rules", bsStyle:"info", icon:"cog", action:this.addRule});
        }
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
                    <Table rowsCount={rows.length}>
                        <Column
                        header={ <Cell>Operation</Cell> }
                        cell={<BtnCell data={rows}
                            btns={btns}/> }
                        width={310}
                        flexGrow={1}/>
                        <Column
                            header={ <Cell> id </Cell> }
                            cell={ <TextCell data={rows} col="id"onDoubleClick={this.openDialogByKey.bind(this,"id")}/>}
                            width={80} />
                        <Column
                            header={ <Cell>dsName</Cell> }
                            cell={ <TextCell data={rows} col="dsName" onDoubleClick={this.openDialogByKey.bind(this,"dsName")}/>}
                            width={150} />
                        <Column
                            header={ <Cell>schemaName</Cell> }
                            cell={ <TextCell data={rows} col="schemaName" onDoubleClick={this.openDialogByKey.bind(this,"schemaName")}/>}
                            width={150} />
                        <Column
                            header={ <Cell>tableName</Cell> }
                            cell={ <TextCell data={rows} col="tableName" onDoubleClick={this.openDialogByKey.bind(this,"tableName")}/>}
                            width={250} />
                        <Column
                            header={ <Cell>status</Cell> }
                            cell={props => (
                            <Cell>
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
                            width={100}
                        />
                        <Column
                            header={ <Cell>History</Cell> }
                            cell={ <TextCell data={rows} col="versionsChangeHistory" onDoubleClick={this.openDialogByKey.bind(this,"versionsChangeHistory")}/> }
                            width={100} />
                        <Column
                            header={ <Cell>version</Cell> }
                            cell={props => (
                            <div onClick={this.openVersionDifference.bind(this, rows[props.rowIndex])} title="Click to View Version History" style={{marginLeft:"8px",marginTop:"13px",fontWeight:"bold",textDecoration:"underline",cursor:"pointer"}}>
                                {rows[props.rowIndex]["version"]}
                            </div>)
                            }
                            width={100} />
                        <Column
                            header={ <Cell>physicalTableRegex</Cell> }
                            cell={ <TextCell data={rows} col="physicalTableRegex"  ref="physicalTableRegex" onDoubleClick={this.openDialogByKey.bind(this,"physicalTableRegex")}/>}
                            width={250} />
                        <Column
                            header={ <Cell>outputTopic</Cell> }
                            cell={ <TextCell data={rows} col="outputTopic" onDoubleClick={this.openDialogByKey.bind(this,"outputTopic")}/>}
                            width={300} />
                        <Column
                            header={ <Cell>namespace</Cell> }
                            cell={ <TextCell data={rows} col="namespace" onDoubleClick={this.openDialogByKey.bind(this,"namespace")}/>}
                            width={450} />
                        <Column
                            header={ <Cell>createTime</Cell> }
                            cell={ <TextCell data={rows} col="createTime" onDoubleClick={this.openDialogByKey.bind(this,"createTime")}/>}
                            width={200}
                            flexGrow={1}/>
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
                                 <br/><br/>
                                 <textarea className="form-control" ref="zkResult"  rows="30" cols="70"></textarea>
                            </Modal.Body>
                            <Modal.Footer>
                                <B.Button onClick={this.closeDialogZK}>Close</B.Button>
                            </Modal.Footer>
                        </Modal>
                        <Modal
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
                    </div>
                </TF.Body>
            </TF>
        );
    }
});

function buildQueryParmeter(p, pageNum) {
    var param = {
        pageSize:10,
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
