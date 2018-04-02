var React = require('react');
var ReactDOM = require('react-dom');
var Reflux = require('reflux');
var B = require('react-bootstrap');
var TF = require('../common/table/tab-frame');
var utils = require('../common/utils');
var Select = require('../common/select');
var store = require('./topologys-store');
var cells = require('../common/table/cells');
var antd = require('antd');
var TopologyUtils = require('./topology-utils');


var Topologys = React.createClass({
    mixins: [Reflux.listenTo(store, "_onStatusChange")],
    getInitialState: function() {
        var state = store.initState();
        return state;
    },
    componentDidMount: function() {
        store.actions.initialLoad(this);
        store.actions.getJarList();
    },

    search: function(value) {
        store.actions.search({dsName: value, stormRest: this.state.stormRest}, this);
    },
    refresh: function () {
        var version = ReactDOM.findDOMNode(this.refs.ds).childNodes[0].value;
        this.search(version);
    },

    stop: function (name, id) {
        var waitTime = prompt("请输入等待时间(秒)", '10');
        if (waitTime == null) return;
        store.actions.stop({id: id, waitTime: waitTime, stormRest: this.state.stormRest});
    },

    start: function () {
        if(this.state.selectedVersion == null) {
            alert("请选择版本");
            return;
        }
        if(this.state.selectedType == null) {
            alert("请选择类型");
            return;
        }
        if(this.state.selectedTime == null) {
            alert("请选择时间");
            return;
        }
        store.actions.start();
    },

    openStartDialog: function (dsInfo) {
        store.actions.openStartDialog(dsInfo);
    },

    closeStartDialog: function () {
        store.actions.closeStartDialog();
    },
    
    _onStatusChange: function(state) {
        this.setState(state);
    },

    handleVersionSelect: function (value) {
        store.actions.handleVersionSelect(value);
    },

    handleTypeSelect: function (value) {
        store.actions.handleTypeSelect(value);
    },

    handleTimeSelect: function (value) {
        store.actions.handleTimeSelect(value);
    },
    
    render: function() {
        var self = this;

        var getAntOptionList = TopologyUtils.getAntOptionList;
        var filterByDsType = TopologyUtils.filterByDsType;
        var getSelectList = TopologyUtils.getSelectList;
        
        var versionSelectList = getSelectList(self.state.jarList);
        versionSelectList = filterByDsType(versionSelectList, self.state.selectedDsInfo);
        versionSelectList = getAntOptionList(versionSelectList, "version");

        var typeSelectList = getSelectList(self.state.jarList, self.state.selectedVersion);
        typeSelectList = filterByDsType(typeSelectList, self.state.selectedDsInfo);
        typeSelectList = getAntOptionList(typeSelectList, "type");


        var timeSelectList = getSelectList(self.state.jarList, self.state.selectedVersion, self.state.selectedType);
        timeSelectList = filterByDsType(timeSelectList, self.state.selectedDsInfo);
        timeSelectList = getAntOptionList(timeSelectList, "time");
        
        return (
            <TF>
                <TF.Header title="Topology Manager">
                    <Select
                        ref="ds"
                        defaultOpt={{value:0, text:"select a data source"}}
                        options={this.state.dsOptions}
                        onChange={this.search}/>
                    <B.Button
                        bsSize="sm"
                        onClick={this.refresh}>
                        Refresh
                    </B.Button>
                </TF.Header>
                <TF.Body>
                    <antd.Table locale={{emptyText:"No topology"}} bordered size="middle"
                                pagination={false}
                                dataSource={this.state.topologys}
                                columns={this.state.topologyColumns} />
                </TF.Body>
                <antd.Modal title="Start Topology" footer={[<antd.Button onClick={this.closeStartDialog}>Close</antd.Button>]}
                            visible={this.state.visible}
                            onCancel={this.closeStartDialog}
                            width={768}>
                    <B.Form horizontal>
                        <B.FormGroup>
                            <B.Col componentClass={B.ControlLabel} sm={2}>
                                Version
                            </B.Col>
                            <B.Col sm={6}>
                                <antd.Select placeholder="Version" value={self.state.selectedVersion} style={{ width: "100%" }} onChange={this.handleVersionSelect}>
                                    {versionSelectList}
                                </antd.Select>
                            </B.Col>
                        </B.FormGroup>
                        <B.FormGroup>
                            <B.Col componentClass={B.ControlLabel} sm={2}>
                                Type
                            </B.Col>
                            <B.Col sm={6}>
                                <antd.Select placeholder="Type" value={self.state.selectedType} style={{ width: "100%" }} onChange={this.handleTypeSelect}>
                                    {typeSelectList}
                                </antd.Select>
                            </B.Col>
                        </B.FormGroup>
                        <B.FormGroup>
                            <B.Col componentClass={B.ControlLabel} sm={2}>
                                minor version
                            </B.Col>
                            <B.Col sm={6}>
                                <antd.Select placeholder="minor version" value={self.state.selectedTime} style={{ width: "100%" }} onChange={this.handleTimeSelect}>
                                    {timeSelectList}
                                </antd.Select>
                            </B.Col>
                            <B.Col sm={1}>
                                <antd.Button type="primary" size="large" loading={self.state.loading} onClick={this.start}>Start</antd.Button>
                            </B.Col>
                        </B.FormGroup>
                        <B.FormGroup>
                            <B.Col componentClass={B.ControlLabel} sm={2}>
                                Log
                            </B.Col>
                            <B.Col sm={10}>
                                <antd.Input.TextArea autosize={{ minRows: 4, maxRows: 8 }} value={self.state.consoleResult}/>
                            </B.Col>
                        </B.FormGroup>
                    </B.Form>
                </antd.Modal>
            </TF>
        );
    }
});

module.exports = Topologys;
