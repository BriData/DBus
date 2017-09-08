var Reflux = require('reflux');
var $ = require('jquery')
var utils = require('../../common/utils');

var actions = Reflux.createActions(['initialLoad','cloneFromTemplates','passParam']);
var store = Reflux.createStore({
    //监听所有的actions
    listenables: [actions],
    state: {
        data: [],
        dsName:'',
        dsType:''
    },
    initState: function() {
        return this.state;
    },
    onInitialLoad: function() {
    },
    onPassParam: function(params) {
        var self = this;
        self.state.dsName = params.dsName;
        self.state.dsType = params.dsType;
        $.get(utils.builPath("zk/loadZkTreeByDsName"), {dsName:self.state.dsName}, function(result) {
            if(result.status !== 200) {
                console.info("result.status !== 200");
                self.state.data = [{name:'加载失败!'}];
                return;
            }
            self.state.data=result.data;
            self.trigger(self.state);
        });
    },

    cloneFromTemplates:function() {
    var self = this;
        var param = {
            dsName:this.state.dsName,
            dsType:this.state.dsType
        };

    $.get(utils.builPath("zk/cloneConfFromTemplate"), param, function(result) {
        if(result.status !== 200)
        {
                //console.error(JSON.stringify(result));
                alert("克隆配置失败！");
                return;
        }else{
            $.get(utils.builPath("zk/loadZkTreeByDsName"), {dsName:self.state.dsName}, function(result) {
                if(result.status !== 200) {
                    console.info("result.status !== 200");
                    self.state.data = [{name:'加载失败!'}];
                    return;
                }
                self.state.data=result.data;
                self.trigger(self.state);
            });
        }
    });
}
});

store.actions = actions;
module.exports = store;
