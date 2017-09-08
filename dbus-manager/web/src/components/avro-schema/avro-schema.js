var React = require('react');
var ReactDOM = require('react-dom');
var Reflux = require('reflux');
var B = require('react-bootstrap');
var Tab = require('fixed-data-table');
var TF = require('../common/table/tab-frame');
var Select = require('../common/select');
var Table = require('../common/table/default-table');
var store = require('./avro-schema-store');
var cells = require('../common/table/cells');
var minxin = require('../common/table/mixin');
var $ = require('jquery');
var utils = require('../common/utils');

var Column = Tab.Column;
var Cell = Tab.Cell;
var TextCell = cells.TextCell;
var LinkCell = cells.LinkCell;
var CheckboxCell = cells.CheckboxCell;
var Modal = B.Modal;

var AvroSchema = React.createClass({
    mixins: [Reflux.listenTo(store, "_onStatusChange"), minxin],
    getInitialState: function() {
        var state = store.initState();
        return state;
    },
    componentDidMount: function() {
        // 监听select-all事件，在CheckboxCell中会发出select-all事件
        this.initSelect(store);
        store.actions.initialLoad();
    },

    // 选择数据源后触发加载schema事件
    dsSelected: function(dsId) {
        store.actions.dataSourceSelected(dsId);
    },
    // 查询按钮响应事件
    search: function(e, pageNum) {
        var p = {
            dsId: this.refs.ds.getValue(),
            schemaName: this.refs.schema.getValue(),
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
    render: function() {
        var rows = this.state.data.list || [];
        //alert("avro  "+rows);
        return (
            <TF>
                <TF.Header title="Avro Schema">
                    <Select
                        ref="ds"
                        defaultOpt={{value:0, text:"select a data srouce"}}
                        options={this.state.dsOptions}
                        onChange={this.dsSelected}/>
                    <Select
                        ref="schema"
                        defaultOpt={{value:0, text:"select a schema"}}
                        options={this.state.schemaOpts}/>
                    <input
                        ref="text"
                        type="text"
                        className="search"
                        placeholder="avro schema name" />
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
                            header={ <CheckboxCell data={rows} status={this.state.select_all_state} emitter={this.emitter} onChange={function(isChecked){console.log("select all checked: "+isChecked)}}> datasource </CheckboxCell> }
                            cell={ <CheckboxCell data={rows} emitter={this.emitter} col="dsName" onChange={function(isChecked, data) {console.log(data);}}/>}
                            width={120} />
                        <Column
                            header={ <Cell>data schema</Cell> }
                            cell={ <LinkCell data={rows} col="namespace" />}
                            width={120} />
                        <Column
                            header={ <Cell>schemaName</Cell> }
                            cell={ <TextCell data={rows} col="schemaName" onDoubleClick={this.openDialogByKey.bind(this,"schemaName")}/>}
                            width={250} />
                        <Column
                            header={ <Cell>schema hash</Cell> }
                            cell={ <TextCell data={rows} col="schemaHash" onDoubleClick={this.openDialogByKey.bind(this,"schemaHash")}/>}
                            width={120} />
                        <Column
                            header={ <Cell>createTime</Cell> }
                            cell={ <TextCell data={rows} col="createTime" onDoubleClick={this.openDialogByKey.bind(this,"createTime")}/>}
                            width={200} />
                        <Column
                            header={ <Cell>schemaText</Cell> }
                            cell={ <TextCell data={rows} col="schemaText" onDoubleClick={this.openDialogByKey.bind(this,"schemaText")} />}
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
                    </div>
                </TF.Body>
            </TF>
        );
    }
});

var Dialog = React.createClass({
    close: function() {
        this.props.closeDialog();
    },
    render: function() {
        var content = this.props.content.replace(/\n/gm, "<br/>");
        content = content.replace(/[' ']/gm, "&nbsp;");
        return (
            <Modal
                show={true}
                onHide={this.close}>
                <Modal.Header closeButton>
                    <Modal.Title>Avro Schema内容</Modal.Title>
                </Modal.Header>
                <Modal.Body>
                    <div dangerouslySetInnerHTML={{__html: content}} ></div>
                </Modal.Body>
                <Modal.Footer>
                    <B.Button onClick={this.close}>Close</B.Button>
                </Modal.Footer>
            </Modal>
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
    if(p.schemaName != 0) {
        param.schemaName = p.schemaName;
    }
    if(p.text != "") {
        param.text = p.text;
    }
    return param;
}

module.exports = AvroSchema;
