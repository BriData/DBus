var React = require('react');
var ReactDOM = require('react-dom');
var $ =require('jquery');
var Reflux = require('reflux');
var B = require('react-bootstrap');
var Tab = require('fixed-data-table');
var TF = require('../common/table/tab-frame');
var Select = require('../common/select');
var Table = require('../common/table/default-table');
var store = require('./dbus-data-store');
var cells = require('../common/table/cells');
var Link = require('react-router').Link;

var Column = Tab.Column;
var Cell = Tab.Cell;
var TextCell = cells.TextCell;
var utils = require('../common/utils');

var Modal = B.Modal;
var StatusCell = cells.StatusCell;
var BtnCell = cells.BtnCell;


var DbusData = React.createClass({
    mixins: [Reflux.listenTo(store, "_onStatusChange")],
    getInitialState: function() {
        var state = store.initState();
        return state;
    },
    componentDidMount: function() {
        store.actions.initialLoad();
    },
    search: function(e, pageNum) {
        utils.showLoading();
        store.actions.search(this.refs.ds.getValue());
    },
    _onStatusChange: function(state) {
        this.setState(state);
    },
    pageChange: function(e, pageNum) {
        this.search(e, pageNum);
    },
    openDialogByKey: function(key, obj) {
        store.actions.openDialogByKey(key, obj);
    },
    openDialogSql:function(data){
        var str1 = [];
        str1 = data.column.split("\n");
        var str2 = [];
        var sql = "select ";
        for(var i =0;i < str1.length;i++){
            str2 = str1[i].split(",");
            sql = sql + str2[0] + ",";
        }
        sql = sql.substring(0,sql.length-2);
        sql = sql + " from " + data.name;
        store.actions.openDialogSql(sql);
    },
    closeDialogColumn: function() {
        store.actions.closeDialogColumn();
    },
    closeDialogSql: function() {
        store.actions.closeDialogSql();
    },
    executeDialogSqlonMaster:function() {
        var sqlParam = {
            dsId:this.refs.ds.getValue(),
            sql:ReactDOM.findDOMNode(this.refs.sqlStatement).value
        }
        store.actions.executeDialogSqlonMaster(sqlParam);
        //this.closeDialogSql();
        utils.showLoading();
    },
    executeDialogSqlonSlave:function() {
        var sqlParam = {
            dsId:this.refs.ds.getValue(),
            sql:ReactDOM.findDOMNode(this.refs.sqlStatement).value
        }
        store.actions.executeDialogSqlonSlave(sqlParam);
        //this.closeDialogSql();
        utils.showLoading();
    },
    render: function() {
        var rows = this.state.data || [];
        return (
            <TF>
                <TF.Header title="Dbus Data ">
                    <Select
                        ref="ds"
                        defaultOpt={{value:0, text:"select a data source"}}
                        options={this.state.dsOptions}
                        onChange={this.search}/>
                </TF.Header>
                <TF.Body pageCount={this.state.data.pages} onPageChange={this.pageChange}>
                    <Table rowsCount={rows.length}>
                        <Column
                            header={ <Cell > 类型 </Cell> }
                            cell={ <TextCell data={rows}  col="type" onDoubleClick={this.openDialogByKey.bind(this,"type")}/>}
                            width={100} />
                        <Column
                            header={<Cell> 名称  </Cell> }
                            cell={ <TextCell data={rows} col="name" onDoubleClick={this.openDialogByKey.bind(this,"name")}/> }
                            width={220} />
                        <Column
                            header={ <Cell> 是否存在 </Cell> }
                            cell={ <TextCell data={rows}  col="exist" onDoubleClick={this.openDialogByKey.bind(this,"exist")}/>}
                            width={100}/>
                        <Column
                            header={ <Cell> 列信息  </Cell> }
                            cell={ <TextCell data={rows} col="column" onDoubleClick={this.openDialogByKey.bind(this,"column")}/>}
                            width={660} />
                        <Column
                            header={ <Cell>操作</Cell> }
                            cell={<BtnCell data={rows}
                            btns={[{text:"查看数据", bsStyle:"info", icon:"log-in", action:this.openDialogSql}
                                   ]}/> }
                            width={100}
                            flexGrow={1}
                        />
                    </Table>



                    <div id="dialogHolder">
                        <Modal
                            show={this.state.dialog.showColumn}
                            onHide={this.closeDialogColumn}>
                            <Modal.Header closeButton>
                                <Modal.Title>{this.state.dialog.identity}</Modal.Title>
                            </Modal.Header>
                            <Modal.Body>
                                <div dangerouslySetInnerHTML={{__html:"<div style='word-wrap: break-word'>"+this.state.dialog.content+"</div>"}}></div>

                            </Modal.Body>
                            <Modal.Footer>
                                <B.Button onClick={this.closeDialogColumn}>Close</B.Button>
                            </Modal.Footer>
                        </Modal>

                        <Modal
                            bsSize="large"
                            show={this.state.dialog.showSql}
                            onHide={this.closeDialogSql}>
                            <Modal.Header closeButton>
                                <Modal.Title>Sql查询语句
                                </Modal.Title>
                            </Modal.Header>
                            <Modal.Body>
                                <textarea className="form-control" ref="sqlStatement"  rows="3" cols="120" style={{resize:"none"}}>{this.state.dialog.content}</textarea>
                                <Modal.Footer>
                                    <B.Button className="btn btn-primary" onClick={this.executeDialogSqlonMaster}>Query Master</B.Button>
                                    <B.Button className="btn btn-primary" onClick={this.executeDialogSqlonSlave}>Query Slave</B.Button>
                                    <B.Button onClick={this.closeDialogSql}>Close</B.Button>
                                </Modal.Footer>
                            </Modal.Body>
                            <Modal.Body>
                                <div>{this.state.tableSql}</div>
                            </Modal.Body>
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

    if(p.dsId != 0) {
        param.dsId = p.dsId;
    }

    if(p.text != "") {
        param.text = p.text;
    }
    return param;
}

module.exports = DbusData;
