import {Table, Input, Icon, Button, InputNumber, Popconfirm, Select, Pagination} from 'antd';
const Option = Select.Option;
var React = require('react');
var ReactDOM = require('react-dom');
var Reflux = require('reflux');
var B = require('react-bootstrap');
var store = require('./config-store');
var JSONEditor = require('jsoneditor');
var $ = require('jquery');
var TF = require('../common/table/tab-frame');
var EditableTable = require('./test');
var EditableTable_1 = require('./edittable');
var utils = require('../common/utils');

var Form = B.Form;
var Col = B.Col;
var FormGroup = B.FormGroup;
var FormControl = B.FormControl;
var ControlLabel = B.ControlLabel;
var editor = null;

var Config = React.createClass({
    mixins: [Reflux.listenTo(store, "_onStatusChange")],
    getInitialState: function () {
        var state = store.initState();
        return state;
    },
    componentDidMount: function () {
        utils.showLoading();
        store.actions.initialLoad();
        store.actions.initialLoadds();
        store.actions.initialLoadschema();

    },
    createDsList: function () {
        var list = [];
        this.state.dsOptions.forEach(function (ds, idx) {
            list.push(ds.value);
        });
        return list;
    },
    child1_setstate: function (child1_state) {
        var heartBeatTimeoutAdditional_temp = [];
        var heartBeatTimeoutAdditional_term = {};
        for (var i = 0; i < child1_state.dataSource.length; i++) {
            var dataSource_ele = {};
            dataSource_ele.startTime = child1_state.dataSource[i].startTime;
            dataSource_ele.endTime = child1_state.dataSource[i].endTime;
            dataSource_ele.heartBeatTimeout = child1_state.dataSource[i].heartBeatTimeout;
            heartBeatTimeoutAdditional_temp = [...heartBeatTimeoutAdditional_temp, dataSource_ele];
            let key = child1_state.dataSource[i].schemaName;
            heartBeatTimeoutAdditional_term[key] = heartBeatTimeoutAdditional_temp[i];
        }
        this.state.data.heartbeat_config.heartBeatTimeoutAdditional = heartBeatTimeoutAdditional_term;
    },

    child_setstate: function (child_state) {
        var additionalNotify_temp = [];
        var additionalNotify_term = {};
        for (var i = 0; i < child_state.dataSource.length; i++) {
            var dataSource_ele = {};
            dataSource_ele.SMSNo = child_state.dataSource[i].SMSNo;
            dataSource_ele.UseSMS = child_state.dataSource[i].UseSMS;
            dataSource_ele.Email = child_state.dataSource[i].Email;
            dataSource_ele.UseEmail = child_state.dataSource[i].UseEmail;
            additionalNotify_temp = [...additionalNotify_temp, dataSource_ele];
            let key = child_state.dataSource[i].schemaName;
            additionalNotify_term[key] = additionalNotify_temp[i];
        }
        this.state.data.heartbeat_config.additionalNotify = additionalNotify_term;
    },

    _onStatusChange: function (state) {
        this.setState(state);
    },
    global_config: function (e) {
        var new_value = e.target.value;
        store.actions.global_config(new_value);
    },
    monitor_config: function (e) {
        var new_value = e.target.value;
        store.actions.monitor_config(new_value);
    },
    storm_config: function (e) {
        var new_value = e.target.value;
        store.actions.storm_config(new_value);
    },
    user_config: function (e) {
        var new_value = e.target.value;
        store.actions.user_config(new_value);
    },
    stormRestChange: function (e) {
        var new_value = e.target.value;
        store.actions.stormRestChange(new_value);
    },
    heartbeatInterval: function (e) {
        var new_value = e.target.value;
        store.actions.heartbeatInterval(new_value);
    },
    checkInterval: function (e) {
        var new_value = e.target.value;
        store.actions.checkInterval(new_value);
    },
    checkFullPullInterval: function (e) {
        var new_value = e.target.value;
        store.actions.checkFullPullInterval(new_value);
    },
    deleteFullPullOldVersionInterval: function (e) {
        var new_value = e.target.value;
        store.actions.deleteFullPullOldVersionInterval(new_value);
    },
    maxAlarmCnt: function (e) {
        var new_value = e.target.value;
        store.actions.maxAlarmCnt(new_value);
    },
    heartBeatTimeout: function (e) {
        var new_value = e.target.value;
        store.actions.heartBeatTimeout(new_value);
    },
    fullPullTimeout: function (e) {
        var new_value = e.target.value;
        store.actions.fullPullTimeout(new_value);
    },
    alarmTtl: function (e) {
        var new_value = e.target.value;
        store.actions.alarmTtl(new_value);
    },
    lifeInterval: function (e) {
        var new_value = e.target.value;
        store.actions.lifeInterval(new_value);
    },
    correcteValue: function (e) {
        var new_value = e.target.value;
        store.actions.correcteValue(new_value);
    },
    fullPullCorrecteValue: function (e) {
        var new_value = e.target.value;
        store.actions.fullPullCorrecteValue(new_value);
    },
    fullPullSliceMaxPending: function (e) {
        var new_value = e.target.value;
        store.actions.fullPullSliceMaxPending(new_value);
    },
    monitorFullPullPaht: function (e) {
        var new_value = e.target.value;
        store.actions.monitorFullPullPaht(new_value);
    },
    excludeSchema: function (e) {
        var new_value = e.target.value;
        store.actions.excludeSchema(new_value);
    },
    checkPointPerHeartBeatCnt: function (e) {
        var new_value = e.target.value;
        store.actions.checkPointPerHeartBeatCnt(new_value);
    },
    fullPullOldVersionCnt: function (e) {
        var new_value = e.target.value;
        store.actions.fullPullOldVersionCnt(new_value);
    },
    adminSMSNo: function (e) {
        var new_value = e.target.value;
        store.actions.adminSMSNo(new_value);
    },
    adminUseSMS: function (value) {
        // var new_value = e.target.value;
        console.log(`selected ${value}`);
        store.actions.adminUseSMS(value);
    },
    adminEmail: function (e) {
        var new_value = e.target.value;
        store.actions.adminEmail(new_value);
    },
    adminUseEmail: function (value) {
        console.log(`selected ${value}`);
        store.actions.adminUseEmail(value);
    },
    schemaChangeEmail: function (e) {
        var new_value = e.target.value;
        store.actions.schemaChangeEmail(new_value);
    },
    schemaChangeUseEmail: function (value) {
        console.log(`selected ${value}`);
        store.actions.schemaChangeUseEmail(value);
    },
    checkStormAvailable: function () {
        var p = this.state.data.global_config;
        store.actions.checkStormAvailable(p);
    },

    savezk: function (data, e) {
        var p = this.state.data.global_config;
        store.actions.savezk(p);
    },

    save_heart_conf: function (data, e) {
        var p = this.state.data.heartbeat_config;
        var pp_1 = this.state.data.heartbeat_config.heartBeatTimeoutAdditional;
        for (var key in pp_1) {
            if (key == "000") {
                alert("存在格式不正确的表名，不能保存，请检查");
                return;
            }
        }
        for (var key in pp_1) {
            if (pp_1[key].startTime == "000" || pp_1[key].endTime == "000") {
                alert("存在格式不正确的时间，不能保存，请检查");
                return;
            }
        }
        var pp_2 = this.state.data.heartbeat_config.additionalNotify;
        for (var key in pp_2) {
            if (key == "000") {
                alert("存在格式不正确的表名，不能保存，请检查");
                return;
            }
        }
        for (var key in pp_2) {
            if (pp_2[key].Email == "000") {
                alert("存在格式不正确的邮箱地址，不能保存，请检查");
                return;
            }
        }
        store.actions.save_heart_conf(p);
    },
    edit: function () {
        this.setState({editable: true});
    },
    to_array: function (data) {
        var array_adt = Object.keys(data).map(function (el) {
            return data[el];
        });
        return array_adt;
    },
    render: function () {
        // defaultValue只会在第一次获得值的时候起作用，所以必须确保已经从后台获取到了配置信息才可以渲染
        if (this.state.data == null) return null;
        return (
            <div className="container-fluid">

                <div className="row header">
                    <h4 className="col-xs-12">
                        Global-Config
                    </h4>
                    <h4 className="col-xs-12">

                    </h4>
                </div>
                <div className="row body">
                    <Form horizontal>
                        <FormGroup>
                            <Col componentClass={ControlLabel} sm={2}>
                                bootstrap.servers
                            </Col>
                            <Col sm={3}>
                                <Input
                                    defaultValue={this.state.data.global_config.bootstrap}
                                    onChange={this.global_config}
                                    placeholder="kafka配置"
                                    type="textarea"
                                    size="big"/>
                            </Col>
                            <div style={{color:"#999",fontSize: 13}}>
                                <Col sm={3}>(kafka配置)</Col>
                            </div>
                        </FormGroup>

                        <FormGroup>
                            <Col componentClass={ControlLabel} sm={2}>
                                zookeeper
                            </Col>
                            <Col sm={3}>
                                <Input
                                    defaultValue={this.state.data.global_config.zookeeper}
                                    type="text"
                                    readOnly/>
                            </Col>
                            <div style={{color:"#999",fontSize: 13}}>
                                <Col sm={3}>(zk 地址)</Col>
                            </div>
                        </FormGroup>

                        <FormGroup>
                            <Col componentClass={ControlLabel} sm={2}>
                                Monitor
                            </Col>
                            <Col sm={3}>
                                <Input
                                    defaultValue={this.state.data.global_config.monitor_url}
                                    onChange={this.monitor_config}
                                    placeholder="监控url"
                                    type="textarea"
                                    focus/>
                            </Col>
                            <div style={{color:"#999",fontSize: 13}}>
                                <Col sm={3}>(监控url)</Col>
                            </div>
                        </FormGroup>

                        <FormGroup>
                            <Col componentClass={ControlLabel} sm={2}>
                                stormStartScriptPath
                            </Col>
                            <Col sm={3}>
                                <Input
                                    id="stormStartScriptPath"
                                    defaultValue={this.state.data.global_config.storm}
                                    onChange={this.storm_config}
                                    placeholder="storm启动脚本路径"
                                    type="textarea"/>
                            </Col>
                            <div style={{color:"#999",fontSize: 13}}>
                                <Col sm={3}>(storm启动脚本路径)</Col>
                            </div>
                        </FormGroup>

                        <FormGroup>
                            <Col componentClass={ControlLabel} sm={2}>
                                user
                            </Col>
                            <Col sm={3}>
                                <Input
                                    defaultValue={this.state.data.global_config.user}
                                    onChange={this.user_config}
                                    placeholder="storm机器登录用户名"
                                    type="text"/>
                            </Col>
                            <div style={{color:"#999",fontSize: 13}}>
                                <Col sm={3}>(storm机器登录用户名)</Col>
                            </div>
                        </FormGroup>

                        <FormGroup>
                            <Col componentClass={ControlLabel} sm={2}>
                                storm rest api
                            </Col>
                            <Col sm={3}>
                                <Input
                                    id="stormRest"
                                    defaultValue={this.state.data.global_config.stormRest}
                                    onChange={this.stormRestChange}
                                    placeholder="Storm UI REST API"
                                    type="textarea"/>
                            </Col>
                            <div style={{color:"#999",fontSize: 13}}>
                                <Col sm={3}>(Storm UI REST API)</Col>
                            </div>
                        </FormGroup>

                        <div style={{display: "",fontSize: 10}}>
                            <FormGroup>
                                <Col smOffset={9} sm={4}>
                                    <B.Button style={{marginRight: "10px"}} type="button" onClick={this.checkStormAvailable}>
                                        Check_Storm
                                    </B.Button>
                                    <B.Button type="button" bsStyle="primary" onClick={this.savezk}>
                                        Save_Global
                                    </B.Button>
                                </Col>
                            </FormGroup>
                        </div>
                    </Form>
                </div>

                <hr style={{height:"2px",color:'#08c'}}/>

                <div className="row header">
                    <h4 className="col-xs-12">
                        HeartBeat-Config
                    </h4>
                    <h4 className="col-xs-12">

                    </h4>
                </div>

                <div className="row body">
                    <Form horizontal>

                        <div style={{display: ""}}>
                            <FormGroup>
                                <Col componentClass={ControlLabel} sm={2}>
                                    heartbeatInterval
                                </Col>
                                <Col sm={3}>
                                    <Input
                                        defaultValue={this.state.data.heartbeat_config.heartbeatInterval}
                                        onChange={this.heartbeatInterval}
                                        type="number"
                                        step="10"
                                        placeholder="插入心跳间隔时间：S"/>
                                </Col>
                                <div style={{color:"#999",fontSize: 13}}>
                                    <Col sm={3}>(插入心跳间隔：s)</Col>
                                </div>
                            </FormGroup>
                        </div>

                        <div style={{display: ""}}>
                            <FormGroup>
                                <Col componentClass={ControlLabel} sm={2}>
                                    checkInterval
                                </Col>
                                <Col sm={3}>
                                    <Input
                                        defaultValue={this.state.data.heartbeat_config.checkInterval}
                                        onChange={this.checkInterval}
                                        type="number"
                                        step="10"
                                        placeholder="心跳超时检查间隔"/>
                                </Col>
                                <div style={{color:"#999",fontSize: 13}}>
                                    <Col sm={3}>(心跳超时检查间隔：s)</Col>
                                </div>
                            </FormGroup>
                        </div>

                        <div style={{display: ""}}>
                            <FormGroup>
                                <Col componentClass={ControlLabel} sm={2}>
                                    checkFullPullInterval
                                </Col>
                                <Col sm={3}>
                                    <Input
                                        defaultValue={this.state.data.heartbeat_config.checkFullPullInterval}
                                        onChange={this.checkFullPullInterval}
                                        type="number"
                                        step="10"
                                        placeholder="拉取全量检查间隔"/>
                                </Col>
                                <div style={{color:"#999",fontSize: 13}}>
                                    <Col sm={3}>(拉取全量检查间隔：s)</Col>
                                </div>
                            </FormGroup>
                        </div>

                        <div style={{display: ""}}>
                            <FormGroup>
                                <Col componentClass={ControlLabel} sm={2}>
                                    deleteFullPullOldVersionInterval
                                </Col>
                                <Col sm={3}>
                                    <Input
                                        defaultValue={this.state.data.heartbeat_config.deleteFullPullOldVersionInterval}
                                        onChange={this.deleteFullPullOldVersionInterval}
                                        type="number"
                                        step="10"
                                        placeholder="删除全量时间间隔：h"/>
                                </Col>
                                <div style={{color:"#999",fontSize: 13}}>
                                    <Col sm={3}>(删除全量时间间隔：h)</Col>
                                </div>
                            </FormGroup>
                        </div>

                        <div style={{display: ""}}>
                            <FormGroup>
                                <Col componentClass={ControlLabel} sm={2}>
                                    maxAlarmCnt
                                </Col>
                                <Col sm={3}>
                                    <Input
                                        defaultValue={this.state.data.heartbeat_config.maxAlarmCnt}
                                        onChange={this.maxAlarmCnt}
                                        type="number"
                                        step="1"
                                        placeholder="最大报警次数"/>
                                </Col>
                                <div style={{color:"#999",fontSize: 13}}>
                                    <Col sm={3}>(最大报警次数)</Col>
                                </div>
                            </FormGroup>
                        </div>

                        <div style={{display: ""}}>
                            <FormGroup>
                                <Col componentClass={ControlLabel} sm={2}>
                                    heartBeatTimeout
                                </Col>
                                <Col sm={3}>
                                    <Input
                                        defaultValue={this.state.data.heartbeat_config.heartBeatTimeout}
                                        onChange={this.heartBeatTimeout}
                                        type="number"
                                        step="10"
                                        placeholder="心跳超时时间"/>
                                </Col>
                                <div style={{color:"#999",fontSize: 13}}>
                                    <Col sm={3}>(心跳超时时间：ms)</Col>
                                </div>
                            </FormGroup>
                        </div>

                        <div style={{display: "",fontSize: 14}}>
                            <FormGroup>
                                <Col componentClass={ControlLabel} sm={2}>
                                    <div style={{display: ""}}>
                                        heartBeatTimeoutAdditional
                                    </div>
                                    <div style={{display: ""}}>
                                        （心跳超时补充设置）
                                    </div>
                                </Col>
                                <Col sm={8}>
                                    <EditableTable
                                        dataSource={this.state.data.heartbeat_config.heartBeatTimeoutAdditional}
                                        dslist={this.state.dsOptions}
                                        schema={this.state.schema}
                                        callback_parent={this.child1_setstate}
                                    />

                                </Col>
                            </FormGroup>
                        </div>

                        <div style={{display: ""}}>
                            <FormGroup>
                                <Col componentClass={ControlLabel} sm={2}>
                                    fullPullTimeout
                                </Col>
                                <Col sm={3}>
                                    <Input
                                        defaultValue={this.state.data.heartbeat_config.fullPullTimeout}
                                        onChange={this.fullPullTimeout}
                                        type="number"
                                        step="100000"
                                        placeholder="拉取全量超时时间"/>
                                </Col>
                                <div style={{color:"#999",fontSize: 13}}>
                                    <Col sm={3}>(拉取全量超时时间：ms)</Col>
                                </div>
                            </FormGroup>
                        </div>

                        <div style={{display: ""}}>
                            <FormGroup>
                                <Col componentClass={ControlLabel} sm={2}>
                                    alarmTtl
                                </Col>
                                <Col sm={3}>
                                    <Input
                                        defaultValue={this.state.data.heartbeat_config.alarmTtl}
                                        onChange={this.alarmTtl}
                                        type="number"
                                        step="100000"
                                        placeholder="报警超时时间"/>
                                </Col>
                                <div style={{color:"#999",fontSize: 13}}>
                                    <Col sm={3}>(报警超时时间：ms)</Col>
                                </div>
                            </FormGroup>
                        </div>

                        <div style={{display: ""}}>
                            <FormGroup>
                                <Col componentClass={ControlLabel} sm={2}>
                                    lifeInterval
                                </Col>
                                <Col sm={3}>
                                    <Input
                                        defaultValue={this.state.data.heartbeat_config.lifeInterval}
                                        onChange={this.lifeInterval}
                                        type="number"
                                        step="10"
                                        placeholder="心跳生命周期间隔"/>
                                </Col>
                                <div style={{color:"#999",fontSize: 13}}>
                                    <Col sm={3}>(心跳生命周期间隔：s)</Col>
                                </div>
                            </FormGroup>
                        </div>

                        <div style={{display: ""}}>
                            <FormGroup>
                                <Col componentClass={ControlLabel} sm={2}>
                                    correcteValue
                                </Col>
                                <Col sm={3}>
                                    <Input
                                        defaultValue={this.state.data.heartbeat_config.correcteValue}
                                        onChange={this.correcteValue}
                                        type="number"
                                        step="10"
                                        placeholder="心跳不同服务器时间修正值"/>
                                </Col>
                                <div style={{color:"#999",fontSize: 13}}>
                                    <Col sm={3}>(心跳不同服务器时间修正值)</Col>
                                </div>
                            </FormGroup>
                        </div>

                        <div style={{display: ""}}>
                            <FormGroup>
                                <Col componentClass={ControlLabel} sm={2}>
                                    fullPullCorrecteValue
                                </Col>
                                <Col sm={3}>
                                    <Input
                                        defaultValue={this.state.data.heartbeat_config.fullPullCorrecteValue}
                                        onChange={this.fullPullCorrecteValue}
                                        type="number"
                                        step="10"
                                        placeholder="拉取全量不同服务器时间修正值"/>
                                </Col>
                                <div style={{color:"#999",fontSize: 13}}>
                                    <Col sm={3}>(拉取全量不同服务器时间修正值)</Col>
                                </div>
                            </FormGroup>
                        </div>

                        <div style={{display: ""}}>
                            <FormGroup>
                                <Col componentClass={ControlLabel} sm={2}>
                                    fullPullSliceMaxPending
                                </Col>
                                <Col sm={3}>
                                    <Input
                                        defaultValue={this.state.data.heartbeat_config.fullPullSliceMaxPending}
                                        onChange={this.fullPullSliceMaxPending}
                                        type="number"
                                        step="10000"
                                        placeholder="拉取全量kafka offeset无消费最大消息数"/>
                                </Col>
                                <div style={{color:"#999",fontSize: 13}}>
                                    <Col sm={3}>(拉取全量kafka offeset无消费最大消息数)</Col>
                                </div>
                            </FormGroup>
                        </div>

                        <div style={{display: ""}}>
                            <FormGroup>
                                <Col componentClass={ControlLabel} sm={2}>
                                    leaderPath
                                </Col>
                                <Col sm={3}>
                                    <Input
                                        defaultValue={this.state.data.heartbeat_config.leaderPath}
                                        type="text"
                                        readOnly/>
                                </Col>
                                <div style={{color:"#999",fontSize: 13}}>
                                    <Col sm={3}>(心跳主备选举控制路径)</Col>
                                </div>
                            </FormGroup>
                        </div>

                        <div style={{display: ""}}>
                            <FormGroup>
                                <Col componentClass={ControlLabel} sm={2}>
                                    controlPath
                                </Col>
                                <Col sm={3}>
                                    <Input
                                        defaultValue={this.state.data.heartbeat_config.controlPath}
                                        type="text"
                                        readOnly/>
                                </Col>
                                <div style={{color:"#999",fontSize: 13}}>
                                    <Col sm={3}>(心跳控制路径)</Col>
                                </div>
                            </FormGroup>
                        </div>

                        <div style={{display: ""}}>
                            <FormGroup>
                                <Col componentClass={ControlLabel} sm={2}>
                                    monitorPath
                                </Col>
                                <Col sm={3}>
                                    <Input
                                        defaultValue={this.state.data.heartbeat_config.monitorPath}
                                        type="text"
                                        readOnly/>
                                </Col>
                                <div style={{color:"#999",fontSize: 13}}>
                                    <Col sm={3}>(心跳监控路径)</Col>
                                </div>
                            </FormGroup>
                        </div>

                        <div style={{display: ""}}>
                            <FormGroup>
                                <Col componentClass={ControlLabel} sm={2}>
                                    monitorFullPullPath
                                </Col>
                                <Col sm={3}>
                                    <Input
                                        value={this.state.data.heartbeat_config.monitorFullPullPath}
                                        type="text"
                                        readOnly/>
                                </Col>
                                <div style={{color:"#999",fontSize: 13}}>
                                    <Col sm={3}>(拉取全量监控路径)</Col>
                                </div>
                            </FormGroup>
                        </div>

                        <div style={{display: ""}}>
                            <FormGroup>
                                <Col componentClass={ControlLabel} sm={2}>
                                    excludeSchema
                                </Col>
                                <Col sm={3}>
                                    <Input
                                        defaultValue={this.state.data.heartbeat_config.excludeSchema}
                                        onChange={this. excludeSchema}
                                        type="text"
                                        placeholder="不做监控schema"/>
                                </Col>
                                <div style={{color:"#999",fontSize: 13}}>
                                    <Col sm={3}>(不做监控schema)</Col>
                                </div>
                            </FormGroup>
                        </div>

                        <div style={{display: ""}}>
                            <FormGroup>
                                <Col componentClass={ControlLabel} sm={2}>
                                    checkPointPerHeartBeatCnt
                                </Col>
                                <Col sm={3}>
                                    <Input
                                        defaultValue={this.state.data.heartbeat_config.checkPointPerHeartBeatCnt}
                                        onChange={this.checkPointPerHeartBeatCnt}
                                        type="number"
                                        step="1"
                                        placeholder="心跳检查点间隔点数"/>
                                </Col>
                                <div style={{color:"#999",fontSize: 13}}>
                                    <Col sm={3}>(心跳检查点间隔点数)</Col>
                                </div>
                            </FormGroup>
                        </div>

                        <div style={{display: ""}}>
                            <FormGroup>
                                <Col componentClass={ControlLabel} sm={2}>
                                    fullPullOldVersionCnt
                                </Col>
                                <Col sm={3}>
                                    <Input
                                        defaultValue={this.state.data.heartbeat_config.fullPullOldVersionCnt}
                                        onChange={this.fullPullOldVersionCnt}
                                        type="number"
                                        step="1"
                                        placeholder="拉取全量保留版本数"/>
                                </Col>
                                <div style={{color:"#999",fontSize: 13}}>
                                    <Col sm={3}>(拉取全量保留版本数)</Col>
                                </div>
                            </FormGroup>
                        </div>

                        <div style={{display: ""}}>
                            <FormGroup>
                                <Col componentClass={ControlLabel} sm={2}>
                                    adminSMSNo
                                </Col>
                                <Col sm={3}>
                                    <Input
                                        defaultValue={this.state.data.heartbeat_config.adminSMSNo}
                                        onChange={this.adminSMSNo}
                                        type="text"
                                        placeholder="管理者短信号码"/>
                                </Col>
                                <div style={{color:"#999",fontSize: 13}}>
                                    <Col sm={3}>(管理者短信号码)</Col>
                                </div>
                            </FormGroup>
                        </div>

                        <div style={{display: ""}}>
                            <FormGroup>
                                <Col componentClass={ControlLabel} sm={2}>
                                    adminUseSMS
                                </Col>
                                <Col sm={3}>
                                    <Select
                                        defaultValue={this.state.data.heartbeat_config.adminUseSMS}
                                        onChange={this.adminUseSMS}
                                        type="text">
                                        <Option value="N">N</Option>
                                        <Option value="Y">Y</Option>
                                    </Select>
                                </Col>
                                <div style={{color:"#999",fontSize: 13}}>
                                    <Col sm={3}>(管理者是否是使用短信)</Col>
                                </div>
                            </FormGroup>
                        </div>

                        <div style={{display: ""}}>
                            <FormGroup>
                                <Col componentClass={ControlLabel} sm={2}>
                                    adminEmail
                                </Col>
                                <Col sm={3}>
                                    <Input
                                        defaultValue={this.state.data.heartbeat_config.adminEmail}
                                        onChange={this.adminEmail}
                                        type="text"
                                        placeholder="管理者邮箱"/>
                                </Col>
                                <div style={{color:"#999",fontSize: 13}}>
                                    <Col sm={3}>(管理者邮箱)</Col>
                                </div>
                            </FormGroup>
                        </div>

                        <div style={{display: ""}}>
                            <FormGroup>
                                <Col componentClass={ControlLabel} sm={2}>
                                    adminUseEmail
                                </Col>
                                <Col sm={3}>
                                    <Select
                                        defaultValue={this.state.data.heartbeat_config.adminUseEmail}
                                        onChange={this.adminUseEmail}
                                        type="text">
                                        <Option value="N">N</Option>
                                        <Option value="Y">Y</Option>
                                    </Select>
                                </Col>
                                <div style={{color:"#999",fontSize: 13}}>
                                    <Col sm={3}>(管理者是否使用邮箱)</Col>
                                </div>
                            </FormGroup>
                        </div>

                        <div style={{display: ""}}>
                            <FormGroup>
                                <Col componentClass={ControlLabel} sm={2}>
                                    schemaChangeEmail
                                </Col>
                                <Col sm={3}>
                                    <Input
                                        defaultValue={this.state.data.heartbeat_config.schemaChangeEmail}
                                        onChange={this.schemaChangeEmail}
                                        type="text"
                                        placeholder="表结构变更通知邮箱"/>
                                </Col>
                                <div style={{color:"#999",fontSize: 13}}>
                                    <Col sm={3}>(表结构变更通知邮箱)</Col>
                                </div>
                            </FormGroup>
                        </div>

                        <div style={{display: ""}}>
                            <FormGroup>
                                <Col componentClass={ControlLabel} sm={2}>
                                    schemaChangeUseEmail
                                </Col>
                                <Col sm={3}>
                                    <Select
                                        defaultValue={this.state.data.heartbeat_config.schemaChangeUseEmail}
                                        onChange={this.schemaChangeUseEmail}
                                        type="text">
                                        <Option value="N">N</Option>
                                        <Option value="Y">Y</Option>
                                    </Select>
                                </Col>
                                <div style={{color:"#999",fontSize: 13}}>
                                    <Col sm={3}>(是否启用表结构变更通知邮箱)</Col>
                                </div>
                            </FormGroup>
                        </div>

                        <div style={{display: "",fontSize: 14}}>
                            <FormGroup>
                                <Col componentClass={ControlLabel} sm={2}>
                                    <div style={{display: ""}}>
                                        additionalNotify
                                    </div>
                                    <div style={{display: ""}}>
                                        （补充设置）
                                    </div>
                                </Col>
                                <Col sm={8}>
                                    <EditableTable_1
                                        color="#08c"
                                        dslist={this.state.dsOptions}
                                        schema={this.state.schema}
                                        dataSource={this.state.data.heartbeat_config.additionalNotify}
                                        callback_parent={this.child_setstate}
                                    />

                                </Col>
                            </FormGroup>
                        </div>

                        <div style={{display: "",fontSize: 10}}>
                            <FormGroup>
                                <Col smOffset={10} sm={4}>
                                    <B.Button type="button" bsStyle="primary" onClick={this.save_heart_conf}>
                                        Save_Heartbeat
                                    </B.Button>
                                </Col>
                            </FormGroup>
                        </div>

                        <hr style={{height:"2px",color:"#08c"}}/>

                    </Form>
                </div>
            </div>

        );
    }
});

module.exports = Config;
