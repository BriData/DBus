var store = require('./start-topos/store');
var StepCursor = require('./step-cursor');
var React = require('react');
var Reflux = require('reflux');
var Tab = require('fixed-data-table');
var TF = require('../common/table/tab-frame');
var utils = require('../common/utils');
var Table = require('../common/table/default-table');
var cells = require('../common/table/cells');
var Column = Tab.Column;
var Cell = Tab.Cell;
var TextCell = cells.TextCell;
var BtnCell = cells.BtnCell;
var StatusCell = cells.StatusCell;


var StartTopo = React.createClass({
    mixins: [Reflux.listenTo(store, "_onStatusChange")],
    getInitialState: function() {
        var state = store.initState();
        this.passParam();
        return state;
    },
    componentDidMount: function() {
        utils.showLoading();
        store.actions.initialLoad();
    },
    _onStatusChange: function(state) {
        this.setState(state);
    },
    passParam:function() {
        var params = this.props.location.state;
        console.info("params---" + params);
        store.actions.passParam(params);
    },
    startTopology:function(param) {
        utils.showLoading();
        var status = param.status;
        if("running" == status) {
            alert("Topology is Running!");
            utils.hideLoading();
            return;
        }
        store.actions.startTopology(param, this.updateStatus);
    },
    updateStatus:function(flag) {
        if(flag) {
            alert("Topology is started!");
        } else {
            alert("start topology failed!");
        }
    },
    render: function() {
        var rows = this.state.data || [];
        var log = this.state.log;
        return (
            <div className="container-fluid">
                <div className="row header">
                    <h4 className="col-xs-12">Start Topology</h4>
                </div>
                <div className="row header">
                    <div className="col-xs-9">
                    <StepCursor currentStep={3}/><br/>
                    </div>
                </div>
                <div className="step-4-table-left body">
                    <TF>
                        <TF.Body>
                            <Table rowsCount={rows.length}>
                                <Column
                                    header={ <Cell>DS Name</Cell> }
                                    cell={ <TextCell data={rows} col="dsName" />}
                                    width={100} />
                                <Column
                                    header={ <Cell>Topology Type</Cell> }
                                    cell={ <TextCell data={rows} col="topologyType" />}
                                    width={150} />
                                <Column
                                    header={ <Cell>Topology Name</Cell> }
                                    cell={ <TextCell data={rows} col="topologyName" />}
                                    width={180} />
                                <Column
                                    header={ <Cell>Status</Cell> }
                                    cell={ <StatusCell data={rows} styleGetter={function(data) {return data.status == "running" ? "success": "default"}} col="status" />}
                                    width={80} />
                                <Column
                                    header={ <Cell>Storm Path</Cell> }
                                    cell={ <TextCell data={rows} col="stormPath"/>}
                                    width={340}/>
                                <Column
                                    header={ <Cell>Jar Path</Cell> }
                                    cell={ <TextCell data={rows} col="jarPath"/>}
                                    width={300}/>
                                <Column
                                    header={ <Cell>Jar Name</Cell> }
                                    cell={ <TextCell data={rows} col="jarName"/>}
                                    width={300}
                                    flexGrow={1}/>
                                <Column
                                    header={ <Cell>操作</Cell> }
                                    cell={<BtnCell data={rows}
                                    btns={[{text:"启动", bsStyle:"primary", icon:"play", action:this.startTopology}]}/>}
                                    width={100}/>
                            </Table>
                        </TF.Body>
                    </TF>
                </div>
                <div className="panel panel-primary panel-top">
                    <div className="panel-heading">
                        <h3 className="panel-title">Topology start log</h3>
                    </div>
                    <div className="panel-body" dangerouslySetInnerHTML={{__html:log}}>
                    </div>
                </div>
            </div>
    );
    }
});

module.exports = StartTopo;
