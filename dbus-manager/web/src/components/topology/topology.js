var React = require('react');
var ReactDOM = require('react-dom');
var Reflux = require('reflux');
var B = require('react-bootstrap');
var Tab = require('fixed-data-table');
var TF = require('../common/table/tab-frame');
var utils = require('../common/utils');
var Select = require('../common/select');
var Table = require('../common/table/default-table');
var store = require('./topologys-store');
var cells = require('../common/table/cells');
var tcells = require('./topology-cells');

var Column = Tab.Column;
var Cell = Tab.Cell;
var TextCell = cells.TextCell;
var LinkCell = cells.LinkCell;
var CheckboxCell = cells.CheckboxCell;
var SelectCell = tcells.SelectCell;
var BtnCell = cells.BtnCell;
var StatusCell = cells.StatusCell;

var Topologys = React.createClass({
    mixins: [Reflux.listenTo(store, "_onStatusChange")],
    getInitialState: function() {
        var state = store.initState();
        return state;
    },
    componentDidMount: function() {
        utils.showLoading();
        store.actions.initialLoad();
    },

    dsSelected: function(dsId) {
        this._search(1, dsId);
    },

    pageChange: function(e, pageNum) {
        store.actions.search(pageNum, this.refs.ds.getValue());
    },
    _search(pageNum, dsId) {
        var p = {
            pageNum:pageNum,
            pageSize:10,
            dsId: dsId
        };
        store.actions.search(p);
    },
    _onStatusChange: function(state) {
        this.setState(state);
    },
    _onJarChanged: function(data, selected) {
        store.actions.jarChanged(data, selected);
    },
    /*
    _start(data, e) {
        console.log(data);
        //window.setTimeout(function() {utils.hideLoading();}, 1000);
    },
    _stop(data, e) {
        console.log(data);
    },
    */
    addTopology:function(){
        this.props.history.pushState({passParam: "add"}, "/topology-add"); 
    },
    render: function() {
        var rows = this.state.data.list || [];
        return (
            <TF>
                <TF.Header title="Topology">
                    <Select
                        ref="ds"
                        defaultOpt={{value:0, text:"select a data source"}}
                        options={this.state.dsOptions}
                        onChange={this.dsSelected}/>
                    <B.Button
                        bsSize="sm"
                        bsStyle="warning"
                        onClick={this.addTopology}>
                        <span className="glyphicon glyphicon-plus">
                        </span> Add Topology
                    </B.Button>
                </TF.Header>
                <TF.Body pageCount={this.state.data.pages} onPageChange={this.pageChange}>
                    <Table rowsCount={rows.length}>
                        <Column
                            header={ <Cell>DS Name</Cell> }
                            cell={ <TextCell data={rows} col="dsName" />}
                            width={180} />
                        <Column
                            header={ <Cell>Topology Name</Cell> }
                            cell={ <TextCell data={rows} col="topologyName" />}
                            width={250} />
                        <Column
                            header={ <Cell>status</Cell> }
                            cell={ <StatusCell data={rows} styleGetter={function(data) {return data.status == "ACTIVE" ? "success": "default"}} col="status" />}
                            width={120} />
                        <Column
                            header={ <Cell>所在服务器</Cell> }
                            cell={ <TextCell data={rows} col="workers" />}
                            width={200}
                            flexGrow={1}/>
                        <Column
                            header={ <Cell>Jar包信息</Cell> }
                            cell={ <SelectCell data={rows} col="selectedJar" onSelect={this._onJarChanged} />}
                            width={200}
                            flexGrow={1}/>
                        <Column
                            header={ <Cell>操作</Cell> }
                            cell={<BtnCell data={rows}
                            btns={[{text:"启动", bsStyle:"primary", icon:"play", action:this._start},
                                   {text:"停止", bsStyle:"warning", icon:"stop", action:this._stop}]}/>}
                            width={200}/>
                    </Table>
                </TF.Body>
                <div id="x"></div>
            </TF>
        );
    }
});

module.exports = Topologys;
