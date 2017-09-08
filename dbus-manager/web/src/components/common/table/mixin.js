var EventEmitter = require('fbemitter').EventEmitter;
var emitter = new EventEmitter();
var selectAll = null;
var select = null;
module.exports = {
    emitter: emitter,
    initSelect: function(store) {
        var self = this;
        select = emitter.addListener("select", function(e) {
            store.trigger(self.state);
        });

        self.state.select_all_state = false;
        selectAll = emitter.addListener("selectAll", function(e, isChecked) {
            self.state.select_all_state = isChecked;
            store.trigger(self.state);
        });
    },
    componentWillUnmount: function() {
        selectAll.remove();
        select.remove();
    },
};
