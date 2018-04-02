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

            var dispatcherAppenderHighVerPath = result.data.global_config.dispatcherAppenderHighVerPath ;
            var splitterPullerHighVerPath = result.data.global_config.splitterPullerHighVerPath ;
            var logProcessorHighVerPath = result.data.global_config.logProcessorHighVerPath ;
            var extractorHighVerPath = result.data.global_config.extractorHighVerPath ;


            if(splitterPullerHighVerPath != "") {
                var daJarPathFiled = dispatcherAppenderHighVerPath.split("/");
                var daJarName = "";
                console.log("daJarPathFiled.length: " + daJarPathFiled.length);
                if(daJarPathFiled.length == 4) {
                    daJarName = "请确认该路径下是否有jar包！";
                } else {
                    daJarName = daJarPathFiled[4].substring(0, daJarPathFiled[4].indexOf(".jar") + 4);
                }
            } else {
                alert("dispatcher_appender最高版本下无jar包");
            }

            if(splitterPullerHighVerPath != "") {
                var spJarPathFiled = splitterPullerHighVerPath.split("/");
                var spJarName = "";
                console.log("spJarPathFiled.length: " + spJarPathFiled.length);
                if(spJarPathFiled.length == 4) {
                    spJarName = "请确认该路径下是否有jar包！";
                } else {
                    spJarName = spJarPathFiled[4].substring(0, spJarPathFiled[4].indexOf(".jar") + 4);
                }
            } else {
                alert("splitter_puller最高版本下无jar包");
            }

            if(logProcessorHighVerPath != "") {
                var lpJarPathFiled = logProcessorHighVerPath.split("/");
                var lpJarName = "";
                console.log("lpJarPathFiled.length: " + lpJarPathFiled.length);
                if(lpJarPathFiled.length == 4) {
                    lpJarName = "请确认该路径下是否有jar包！";
                } else {
                    lpJarName = lpJarPathFiled[4].substring(0, lpJarPathFiled[4].indexOf(".jar") + 4);
                }
            } else if(utils.logProcessor == self.state.dsType) {
                alert("log_processor最高版本下无jar包！");
            }

            if(extractorHighVerPath != "") {
                var etJarPathFiled = extractorHighVerPath.split("/");
                var etJarName = "";
                console.log("etJarPathFiled.length: " + etJarPathFiled.length);
                if(etJarPathFiled.length == 4) {
                    etJarName = "请确认该路径下是否有jar包！";
                } else {
                    etJarName = etJarPathFiled[4].substring(0, etJarPathFiled[4].indexOf(","));
                }
            } else if("mysql" == self.state.dsType) {
                alert("mysql_extractor最高版本下无jar包！");
            }

            var dahvPathFields = dispatcherAppenderHighVerPath.split("/");
            var sphvPathFields = splitterPullerHighVerPath.split("/");
            var lphvPathFields = logProcessorHighVerPath.split("/");
            var ethvPathFields = extractorHighVerPath.split("/");

            //  0.3.x/extractor/20180111_151058/
            var dispatcherAppenderHighVerPath = dahvPathFields[1] + "/" + dahvPathFields[2] + "/" + dahvPathFields[3] + "/";
            var splitterPullerHighVerPath = sphvPathFields[1] + "/" + sphvPathFields[2] + "/" + sphvPathFields[3] + "/";
            var logProcessorHighVerPath = lphvPathFields[1] + "/" + lphvPathFields[2] + "/" + lphvPathFields[3] + "/";
            var extractorHighVerPath = ethvPathFields[1] + "/" + ethvPathFields[2] + "/" + ethvPathFields[3] + "/";

            console.log("dispatcherAppenderHighVerPath: " + dispatcherAppenderHighVerPath);
            console.log("splitterPullerHighVerPath: " + splitterPullerHighVerPath);
            console.log("logProcessorHighVerPath: " + logProcessorHighVerPath);
            console.log("extractorHighVerPath: " + extractorHighVerPath);

            var dsName = self.state.dsName;
            var dsType = self.state.dsType;
            var TopologyData = [];

            if("mysql" == dsType) {
                TopologyData.push({
                    dsName: dsName,
                    topologyType: "dispatcher-appender",
                    topologyName: dsName + "-dispatcher-appender",
                    status: "inactive",
                    stormPath: stormStartScriptPath,
                    jarPath:  dispatcherAppenderHighVerPath,
                    jarName:daJarName
                });
                TopologyData.push({
                    dsName: dsName,
                    topologyType: "splitter-puller",
                    topologyName: dsName + "-splitter-puller",
                    status: "inactive",
                    stormPath: stormStartScriptPath,
                    jarPath: splitterPullerHighVerPath,
                    jarName: spJarName
                });
                TopologyData.push({
                    dsName: dsName,
                    topologyType: "extractor",
                    topologyName: dsName + "-mysql-extractor",
                    status: "inactive",
                    stormPath: stormStartScriptPath,
                    jarPath: extractorHighVerPath,
                    jarName: etJarName
                });
            } else if("oracle" == dsType) {
                TopologyData.push({
                    dsName: dsName,
                    topologyType: "dispatcher-appender",
                    topologyName: dsName + "-dispatcher-appender",
                    status: "inactive",
                    stormPath: stormStartScriptPath,
                    jarPath:  dispatcherAppenderHighVerPath,
                    jarName:daJarName
                });
                TopologyData.push({
                    dsName: dsName,
                    topologyType: "splitter-puller",
                    topologyName: dsName + "-splitter-puller",
                    status: "inactive",
                    stormPath: stormStartScriptPath,
                    jarPath: splitterPullerHighVerPath,
                    jarName: spJarName
                });
            } else if(utils.logProcessor == dsType) {
                TopologyData.push({
                    dsName: dsName,
                    topologyType: utils.logProcessor,
                    topologyName: dsName + "-" + utils.logProcessor,
                    status: "inactive",
                    stormPath: stormStartScriptPath,
                    jarPath: logProcessorHighVerPath,
                    jarName: lpJarName
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
    onStartTopology: function(param, callback) {
        var self = this;
        var flag = false;
        var dsName = param.dsName;
        var stormPath = param.stormPath;
        var jarPath = param.jarPath;
        var jarName = param.jarName;
        var topologyType = param.topologyType;
        console.log("topologyType: " + topologyType);
        var user = self.state.user;
        var TopologyData = self.state.data;
        self.state.log = "";

        var p = {
            user: user,
            dsName: dsName,
            stormPath: stormPath,
            jarPath: jarPath,
            jarName: jarName,
            topologyType: topologyType
        };
        $.get(utils.builPath("startTopology/startTopo"), p, function(result) {
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
                } else if (utils.logProcessor == param.topologyType) {
                    TopologyData.forEach(function (e) {
                        if (utils.logProcessor == e.topologyType) {
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
