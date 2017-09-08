var $ = require('jquery')
var utils = require('../../common/utils');

var scriptGenerator = {
    createScript: function (store, schemaName) {
        var param = {
            state: JSON.stringify(store.state),
            schemaName: schemaName
        };
        $.post(utils.builPath("tableMeta/createScript"), param, function(result) {
            if (result.status == 200) {
                store.state.dialog.show = true;
                store.state.dialog.content = result.content;
                store.trigger(store.state);
            }
        });
    }
};

module.exports = scriptGenerator;