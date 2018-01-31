var Reflux = require('reflux');
var $ = require('jquery')
var utils = require('../common/utils');
var actions = Reflux.createActions(['initialLoad','openDialog','closeDialog','deleteJar','dbusVersionSelected','dbusJarTypeSelected','setVisible']);

var store = Reflux.createStore({
    //监听所有的actions
    listenables: [actions],
    state: {
        jarList: [],
        validJarList: [],
        dialog: {
            show: false,
            content: "",
            path: ""
        },
        dbusVersion: [],
        dbusJarType: [],
        dbus2JarType: [],
        dbus3JarType: [],

        JarUploadPath: "",
        visible: false
    },
    initState: function() {
        var self = this;
        var dbusVersion = [];

        if(global.isDebug) {
            dbusVersion.push({value: "0.2.x", text: "0.2.x"});
            dbusVersion.push({value: "0.3.x", text: "0.3.x"});
        }


        dbusVersion.push({value: "0.4.x", text: "0.4.x"});

        var dbus2JarType = [];

        if(global.isDebug) {
            //<!-- #opensource_remove_begin# -->
            dbus2JarType.push({value: "dispatcher", text: "dispatcher"});
            dbus2JarType.push({value: "appender", text: "appender"});
            dbus2JarType.push({value: "splitter_puller", text: "splitter_puller"});
            dbus2JarType.push({value: "splitter", text: "splitter"});
            dbus2JarType.push({value: "puller", text: "puller"});
            //<!-- #opensource_remove_end# -->
        }

        

        var dbus3JarType = [];
        if(global.isDebug) {
            dbus3JarType.push({value: "dispatcher", text: "dispatcher"});
            dbus3JarType.push({value: "appender", text: "appender"});
            dbus3JarType.push({value: "splitter", text: "splitter"});
            dbus3JarType.push({value: "puller", text: "puller"});
        }
        dbus3JarType.push({value: "dispatcher_appender", text: "dispatcher_appender"});
        dbus3JarType.push({value: "splitter_puller", text: "splitter_puller"});
        dbus3JarType.push({value: "log_processor", text: "log_processor"});
        dbus3JarType.push({value: "mysql_extractor", text: "mysql_extractor"});

        self.state.dbusVersion = dbusVersion;
        self.state.dbus2JarType = dbus2JarType;
        self.state.dbus3JarType = dbus3JarType;
        self.trigger(self.state);
        return this.state;
    },
    onInitialLoad: function() {
        utils.showLoading();
        var self = this;
        var jarList = [];
        var validJarList = [];
        var finalJarList = [];
        $.get(utils.builPath("jarManager/getJarList"), function(result) {
            if(result.status !== 200) {
                if(console)
                    console.error(JSON.stringify(result));
                alert("get jar list failed！");
                return;
            }
            console.log("data = " + JSON.stringify(result.data));
            result.data.forEach(function(e) {
                var index = e.val.indexOf(",");
                if(e.val.indexOf(".jar") != -1) {
                    jarList.push({path:e.val.substring(0, e.val.indexOf(".jar") + 4), desc:e.val.substring(e.val.indexOf(".jar") + 5, e.val.length)});
                    validJarList.push({path:e.val.substring(0, e.val.indexOf(".jar") + 4), desc:e.val.substring(e.val.indexOf(".jar") + 5, e.val.length)});
                } else {
                    var pathField = e.val.split("/");
                    var path = pathField[0] + "/" + pathField[1] + "/" + pathField[2] + "/" + pathField[3];
                    jarList.push({path:path, desc:""});
                    console.log("路径：" + path + "下没有jar文件！");
                }
            });

            jarList.forEach(function (e) {
                var jarPathFiled = e.path.split("/");
                var jarName = "";
                console.log("e.path length: " + e.path + "   " + jarPathFiled.length);
                if(jarPathFiled.length == 4) {
                    jarName = "请确认该路径下是否有jar包！";
                } else {
                    jarName = jarPathFiled[4];
                }
                finalJarList.push({path:e.path, version:jarPathFiled[1], type:jarPathFiled[2], timestamp:jarPathFiled[3], jarName:jarName, desc:e.desc});
            });


            //按时间戳排序
            var compare = function (a, b) {
                if (typeof a === "object" && typeof b === "object" && a && b) {
                    if (a.version != b.version) return a.version > b.version ? -1 : 1;
                    if (a.type != b.type) return a.type < b.type ? -1 : 1;
                    if (a.timestamp != b.timestamp) return a.timestamp > b.timestamp ? -1 : 1;
                    return 0;
                }
                else {
                    throw ("error");
                }
            };
            finalJarList.sort(compare);
            
            self.state.jarList = finalJarList;
            self.state.validJarList = validJarList;
            self.trigger(self.state);
            utils.hideLoading();
        });
    },

    onDeleteJar: function(param) {
        var self = this;
        utils.showLoading();
        $.get(utils.builPath("jarManager/deleteJar"), param, function(result) {
            if(result.status !== 200) {
                if(console)
                    console.error(JSON.stringify(result));
                alert("delete jar list failed！");
                return;
            }
            console.log("data = " + result.data);
            if(result.data == "") {
                alert("delete success!");
            } else {
                alert("delete failed! Error: " + JSON.stringify(result.data));
            }
            self.trigger(self.state);
            utils.hideLoading();
            self.onInitialLoad();
        });
    },

    onOpenDialog: function(path, description) {
        this.state.dialog.path = path;
        this.state.dialog.content = description;
        this.state.dialog.show = true;
        this.trigger(this.state);
    },

    onSetVisible: function(status) {
        this.state.visible = status;
        this.trigger(this.state);
    },

    onDbusVersionSelected: function(dbusVersion, dbusJarType) {
        var self = this;
        console.log("onDbusJarTypeSelected:  " + dbusVersion + "  " + dbusJarType);
        var dbusJarTypeList = [];
        if(dbusVersion == "0.2.x") {
            dbusJarTypeList = self.state.dbus2JarType;
        } else if(dbusVersion == "0.3.x") {
            dbusJarTypeList = self.state.dbus3JarType;
        } else if(dbusVersion == "0.4.x") {
            dbusJarTypeList = self.state.dbus3JarType;
        } else {
            alert("dbusVersion is not 0.2.x, 0.3.x or 0.4.x !");
        }


        var myDate = new Date();
        var month = myDate.getMonth();
        var date = myDate.getDate();
        var hours = myDate.getHours();
        var minutes = myDate.getMinutes();
        var seconds = myDate.getSeconds();

        if(month < 9) {
            month = month + 1;
            month = "0" + month;
        }
        if(month == 9) month = 10;
        if(month == 10 || month == 11) month = month + 1;

        if(date <= 9) {
            date = "0" + date;
        }

        if(hours <= 9) {
            hours = "0" + hours;
        }

        if(minutes <= 9) {
            minutes = "0" + minutes;
        }

        if(seconds <= 9) {
            seconds = "0" + seconds;
        }

        var time = myDate.getFullYear() + month + date + "_" + hours + minutes + seconds;
        var JarUploadPath = "./" + dbusVersion + "/" + dbusJarType + "/" + time;
        console.log("JarUploadPath: " + JarUploadPath);
        this.state.JarUploadPath = JarUploadPath;
        this.state.dbusJarType = dbusJarTypeList;
        this.trigger(this.state);
    },

    onDbusJarTypeSelected: function(dbusVersion, dbusJarType) {
        console.log("onDbusJarTypeSelected:  " + dbusVersion + "  " + dbusJarType);
        var myDate = new Date();
        var month = myDate.getMonth();
        var date = myDate.getDate();
        var hours = myDate.getHours();
        var minutes = myDate.getMinutes();
        var seconds = myDate.getSeconds();

        if(month < 9) {
            month = month + 1;
            month = "0" + month;
        }
        if(month == 9) month = 10;
        if(month == 10 || month == 11) month = month + 1;

        if(date <= 9) {
            date = "0" + date;
        }

        if(hours <= 9) {
            hours = "0" + hours;
        }

        if(minutes <= 9) {
            minutes = "0" + minutes;
        }

        if(seconds <= 9) {
            seconds = "0" + seconds;
        }

        var time = myDate.getFullYear() + month + date + "_" + hours + minutes + seconds;

        var JarUploadPath = "./" + dbusVersion + "/" + dbusJarType + "/" + time;
        console.log("JarUploadPath: " + JarUploadPath);
        this.state.JarUploadPath = JarUploadPath;
        this.trigger(this.state);
    },

    onCloseDialog: function(path, description) {
        var self = this;
        self.state.dialog.show = false;
        self.trigger(self.state);
        var description = description;
        var p = {
            path:path,
            description:description
        };
        console.log("p: " + p);
        $.get(utils.builPath("jarManager/modifyDesc"), p, function(result) {
            if(result.status !== 200) {
                if(console)
                    console.error(JSON.stringify(result));
                alert("modify description failed！");
                return;
            }
            else{
                //alert("modify success!");
            }
            self.onInitialLoad();
        });
    }
});

store.actions = actions;
module.exports = store;
