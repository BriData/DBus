var Reflux = require('reflux');
var $ = require('jquery')
var utils = require('../../common/utils');
var scriptGenerator = require('../../common/configScript/config-script-generator');

var actions = Reflux.createActions(['initialLoad', 'openDialog', 'closeDialog', 'okDialog']);
var store = Reflux.createStore({
    //监听所有的actions
    listenables: [actions],
    state: {
        tables: [],
        dialog: {
            show: false
        }
    },
    initState: function () {
        return this.state;
    },
    onInitialLoad: function (passParam) {
        var self = this;
        $.get(utils.builPath("tableMeta/listPlainlogTable"), {
                schemaId: passParam.schemaId
            },
            function (result) {
                if (result.status !== 200) {
                    console.error(JSON.stringify(result));
                    alert("Load table fail！");
                    return;
                }
                var tables = [];
                result.data.forEach(function (e) {
                    tables.push({
                        tableName: e.tableName,
                        outputTopic: e.outputTopic,
                        createTime: e.createTime
                    });
                });
                self.state.tables = tables;
                self.trigger(self.state);
            });
    },
    onOpenDialog: function () {
        this.state.dialog.show = true;
        this.trigger(this.state);
    },
    onCloseDialog: function () {
        this.state.dialog.show = false;
        this.trigger(this.state);
    },
    onOkDialog: function (param) {
        var self = this;
        $.get(utils.builPath("tableMeta/createPlainlogTable"), param, function (result) {
            if (result.status !== 200) {
                console.error(JSON.stringify(result));
                alert("Create new table fail！");
                return;
            }
            var tables = [];

            result.data.forEach(function (e) {
                tables.push({
                    tableName: e.tableName,
                    outputTopic: e.outputTopic,
                    createTime: e.createTime
                });
            });
            self.state.tables = tables;
            self.state.dialog.show = false;
            self.trigger(self.state);
        });
    }
});

store.actions = actions;
module.exports = store;
