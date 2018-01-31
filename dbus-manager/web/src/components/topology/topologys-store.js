var React = require('react');
var Reflux = require('reflux');
var $ = require('jquery')
var utils = require('../common/utils');
var antd = require('antd');
var actions = Reflux.createActions(['initialLoad', 'search', 'stop', 'start', 'getJarList', 'openStartDialog', 'closeStartDialog','handleVersionSelect','handleTypeSelect','handleTimeSelect']);
var TopologyUtils = require("./topology-utils");

var store = Reflux.createStore({
    //监听所有的actions
    listenables: [actions],
    state: {
        dsOptions: [],
        topologys: [],
        topologyColumns: [],
        jarList: [],
        stormRest: null,

        loading: false,
        visible: false,
        selectedDsInfo: null,
        selectedVersion: null,
        selectedType: null,
        selectedTime: null,
        consoleResult: null
    },
    initState: function() {
        return this.state;
    },
    onInitialLoad: function(thisView) {
        var self = this;


        $.get(utils.builPath("ds/search"), {}, function(result) {
            if(result.status !== 200) {
                alert("加载数据源失败");
                return;
            }
            var list = [];
            result.data.list.forEach(function(e) {
                list.push({
                    value: e.dsName,
                    text: [e.dsType, e.dsName].join("/"),
                    dsName: e.dsName,
                    dsType: e.dsType
                });
            });
            self.state.dsOptions = list;
            self._getStormRest(thisView);
        });

    },

    _getStormRest(thisView) {
        var self = this;
        $.get(utils.builPath("config/initialLoad"), {}, function(result) {
            if(result.status !== 200) {
                alert("加载storm rest失败");
                return;
            }
            self.state.stormRest = result.data.global_config.stormRest;
            self.onSearch({stormRest: self.state.stormRest}, thisView);
        });
    },

    onHandleVersionSelect: function (value) {
        var self = this;
        self.state.selectedVersion = value;
        self.state.selectedType = null;
        self.state.selectedTime = null;
        self.trigger(self.state);
    },

    onHandleTypeSelect: function (value) {
        var self = this;
        self.state.selectedType = value;
        self.state.selectedTime = null;
        self.trigger(self.state);
    },

    onHandleTimeSelect: function (value) {
        var self = this;
        self.state.selectedTime = value;
        self.trigger(self.state);
    },

    onGetJarList: function() {
        var self = this;
        $.get(utils.builPath("jarManager/getJarList"), {}, function(result) {
            if(result.status !== 200) {
                alert("加载数据源失败");
                return;
            }
            var data = result.data;
            var jarList = data.map(function (item) {
                var wholePath = item.val.split(",")[0];
                var parts = wholePath.split("/");
                return {
                    prefix: parts[0],
                    version: parts[1],
                    type: parts[2],
                    time: parts[3],
                    name: parts[4],
                    wholePath: wholePath
                };
            });
            self.state.jarList = jarList;
            self.trigger(self.state);
        });
    },

    onOpenStartDialog: function (dsInfo) {
        var self = this;
        self.state.visible = true;
        self.state.selectedDsInfo = dsInfo;
        self.state.consoleResult = null;
        self.state.loading = false;
        self.trigger(self.state);
    },

    onCloseStartDialog: function () {
        var self = this;
        self.state.visible = false;
        self.state.selectedDsInfo = null;
        self.state.loading = false;
        self.state.selectedVersion= null;
        self.state.selectedType= null;
        self.state.selectedTime= null;
        self.state.consoleResult= null;
        self.trigger(self.state);
    },
    
    onStart: function() {
        var self = this;
        self.state.loading = true;
        self.trigger(self.state);
        $.get(utils.builPath("startTopology/getPath"), null, function(result) {
            if(result.status !== 200) {
                alert("获取storm配置信息失败");
                self.state.loading = false;
                self.trigger(self.state);
                return;
            }
            var config = result.data.global_config;
            self._sendStart(config);
        });
    },

    _sendStart: function (config) {
        var self = this;

        var jar = TopologyUtils.getSelectList(self.state.jarList, self.state.selectedVersion, self.state.selectedType, self.state.selectedTime)[0];
        var param = {
            path: jar.wholePath,
            mainClass: TopologyUtils.getMainClass(jar),
            tid: TopologyUtils.getTid(self.state.selectedDsInfo.dsName, jar),
            type: TopologyUtils.getType(jar),
            stormStartScriptPath: config.stormStartScriptPath,
            user: config.user
        };

        $.get(utils.builPath("topology/start"), param, function(result) {
            console.log(result);
            if (result.status !== 200)
                alert("启动storm topology失败");
            else
                alert("启动storm topology成功");
            self.state.loading = false;
            self.state.consoleResult = result.data;
            self.trigger(self.state);
        });
    },


    onStop: function (param) {
        $.get(utils.builPath("topology/stop"), param, function(result) {
            if(result.status !== 200) {
                alert("发送Kill请求失败");
                return;
            }
            var status = JSON.parse(result.data).status;
            if(status !== 'success') {
                alert("发送Kill请求失败");
                return;
            }
            alert("发送Kill请求成功");
        });
    },
    onSearch: function(searchParam, thisView){
        var self = this;
        $.get(utils.builPath("topology/list"), searchParam, function(result) {
            if(result.status !== 200) {
                if(console)
                    console.error(JSON.stringify(result));
                alert("加载Storm Topology失败");
                utils.hideLoading();
                return;
            }
            var runningStormTopoList = JSON.parse(result.data).topologies;

            var topologys = [];
            self.state.dsOptions.forEach(function (option) {
                if (searchParam.dsName == null || searchParam.dsName == '0' || searchParam.dsName == option.dsName) {
                    var item = {
                        key: option.value,
                        dsName: option.dsName,
                        dsType: option.dsType,
                        runningTopoList: []
                    };
                    runningStormTopoList.forEach(function (topo) {
                        if (topo.name.split('-')[0] == option.dsName) {
                            item.runningTopoList.push({
                                id: topo.id,
                                name: topo.name,
                                uptime: topo.uptime
                            })
                        }
                    });
                    topologys.push(item);
                }
            });
            self.state.topologys = topologys;

            var topologyColumns = [{
                title: 'Operation',
                width: 75,
                key: 'operation',
                render: (text) => (<antd.Button size="default"
                                               onClick={() => thisView.openStartDialog(text)}>Start Topology</antd.Button>)
            }, {
                title: 'dsName',
                dataIndex: 'dsName',
                key: 'dsName'
            }, {
                title: 'dsType',
                dataIndex: 'dsType',
                key: 'dsType'
            }, {
                title: 'Running Topology',
                key: 'RunningTopology',
                render: (text) => {
                    var runningTopoList = text.runningTopoList;
                    var ret = runningTopoList.map(function(topo, index){
                        return <div>
                                    <span>{topo.name}</span>
                                    <span style={{marginLeft: '5px'}}><a onClick={() => thisView.stop(topo.name, topo.id)} style={{textDecoration: 'underline'}}>Kill</a></span>
                               </div>;
                    });
                    return ret;
                }
            }, {
                title: 'Running Time',
                key: 'RunningTime',
                width: '140px',
                render: (text) => {
                    var runningTopoList = text.runningTopoList;
                    var ret = runningTopoList.map(function(topo, index){
                        return <div>{topo.uptime}</div>;
                    });
                    return ret;
                }
            }];
            self.state.topologyColumns = topologyColumns;
            self.trigger(self.state);
        });
    }
});

store.actions = actions;
module.exports = store;
