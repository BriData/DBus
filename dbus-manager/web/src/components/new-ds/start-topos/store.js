var Reflux = require('reflux');
var $ = require('jquery')
var utils = require('../../common/utils');

var actions = Reflux.createActions(['initialLoad','passParam','startTopology']);
var store = Reflux.createStore({
    //监听所有的actions
    listenables: [actions],
    state: {
        dsName:'',
        dsType:'',
        user:'',
        dispatcher_appender_path:'',
        splitter_puller_path:'',
        extractor_path:'',
        data:[],
        log:""
    },
    initState: function() {
        return this.state;
    },
    onInitialLoad: function() {
        var self = this;
        $.get(utils.builPath("startTopology/getPath"), function(result) {
            if(result.status !== 200) {
                if(console)
                    console.error(JSON.stringify(result));
                alert("Load topology failed！");
                return;
            }
            console.log("self.state.dispatcher_appender_path = " + JSON.stringify(result.data));

            var user = result.data.global_config.user;
            var stormStartScriptPath = result.data.global_config.stormStartScriptPath;
            
            var dsName = self.state.dsName;
            var dsType = self.state.dsType;
            var TopologyData = [];

            if("mysql" == dsType) {
                TopologyData.push({
                    dsName: dsName,
                    topologyType: "dispatcher-appender",
                    topologyName: dsName + "-dispatcher-appender",
                    status: "inactive",
                    path: stormStartScriptPath
                });
                TopologyData.push({
                    dsName: dsName,
                    topologyType: "splitter-puller",
                    topologyName: dsName + "-splitter-puller",
                    status: "inactive",
                    path: stormStartScriptPath
                });
                TopologyData.push({
                    dsName: dsName,
                    topologyType: "extractor",
                    topologyName: dsName + "-mysql-extractor",
                    status: "inactive",
                    path: stormStartScriptPath
                });
            } else {
                TopologyData.push({
                    dsName: dsName,
                    topologyType: "dispatcher-appender",
                    topologyName: dsName + "-dispatcher-appender",
                    status: "inactive",
                    path: stormStartScriptPath
                });
                TopologyData.push({
                    dsName: dsName,
                    topologyType: "splitter-puller",
                    topologyName: dsName + "-splitter-puller",
                    status: "inactive",
                    path: stormStartScriptPath
                });
            }

            self.state.log = "";
            self.state.user = user;
            self.state.data = TopologyData;
            self.trigger(self.state);
            utils.hideLoading();
        });
    },
    onPassParam: function(params) {
        var self = this;
        console.info("params passed by---" + params.dsName);
        self.state.dsName = params.dsName;
        self.state.dsType = params.dsType;
        self.trigger(self.state);
    },
    onStartTopology: function(param,callback) {
        var self = this;
        var flag = false;
        var dsName = param.dsName;
        var path = param.path;
        var topologyType = param.topologyType;
        console.log("topologyType: " + topologyType);
        var user = self.state.user;
        var TopologyData = self.state.data;
        self.state.log = "";

        var param = {
            user:user,
            dsName:dsName,
            path:path,
            topologyType:topologyType
        }
        $.get(utils.builPath("startTopology/startTopo"), param, function(result) {
            if(result.status !== 200) {
                var data = result.data.toString().replace(/\n/gm, "<br/>");
                self.state.log = data;
                self.trigger(self.state);
                callback(flag);
                utils.hideLoading();
                return;
            }
            
            var data = result.data.toString().replace(/\n/gm, "<br/>");
            console.log("startTopology/startTopo data: " + data);
            var idx = data.indexOf("Finished submitting topology");
            console.log("idx: " + idx);

            if(idx != -1) {
                flag = true;
                if ("dispatcher-appender" == param.topologyType) {
                    TopologyData.forEach(function (e) {
                        if ("dispatcher-appender" == e.topologyType) {
                            e.status = "running";
                        }
                    });
                } else if ("splitter-puller" == param.topologyType) {
                    TopologyData.forEach(function (e) {
                        if ("splitter-puller" == e.topologyType) {
                            e.status = "running";
                        }
                    });
                } else if ("extractor" == param.topologyType) {
                    TopologyData.forEach(function (e) {
                        if ("extractor" == e.topologyType) {
                            e.status = "running";
                        }
                    });
                }
                self.state.log = data;
                self.state.data = TopologyData;
                self.trigger(self.state);
                callback(flag);
            } else {
                self.state.log = data;
                self.state.data = TopologyData;
                self.trigger(self.state);
                callback(flag);
            }
            self.trigger(self.state);
            utils.hideLoading();
        });
        self.trigger(self.state);
    }
});

store.actions = actions;
module.exports = store;
