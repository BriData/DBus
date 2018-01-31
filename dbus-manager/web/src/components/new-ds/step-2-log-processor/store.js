var Reflux = require('reflux');
var $ = require('jquery')
var utils = require('../../common/utils');
var scriptGenerator = require('../../common/configScript/config-script-generator');

var actions = Reflux.createActions(['initialLoad', 'nextStep', 'openDialog', 'closeDialog', 'okDialog']);
var store = Reflux.createStore({
    //监听所有的actions
    listenables: [actions],
    state: {
        newOutputTopic: '',
        tables: [],
        dialog: {
            show: false
        }
    },
    initState: function (dsName, schemaName) {
        return this.state;
    },
    onNextStep: function (param, callbackRedirect) {
        utils.showLoading();
        $.get(utils.builPath("tableMeta/createPlainlogSchemaAndTable"), param, function (result) {
            utils.hideLoading();
            if (result.status !== 200) {
                console.error(JSON.stringify(result));
                alert("Save table fail！");
                return;
            }
            callbackRedirect();
        });
    },
    onOpenDialog: function (newOutputTopic) {
        this.state.newOutputTopic = newOutputTopic;
        this.state.dialog.show = true;
        this.trigger(this.state);
    },
    onCloseDialog: function () {
        this.state.dialog.show = false;
        this.trigger(this.state);
    },
    onOkDialog: function (param) {
        this.state.tables.push(param);
        this.state.dialog.show = false;
        this.trigger(this.state);
    }
});

store.actions = actions;
module.exports = store;
