//http://dbus-grafana.creditease.corp/dashboard/db/schema-statistic-board

var React = require('react');
var ReactDOM = require('react-dom');
var Reflux = require('reflux');
var store = require('./monitor-store');
var utils = require('../common/utils');
var $ = require('jquery');

     
var Monitor = React.createClass({
    mixins: [Reflux.listenTo(store, "_onStatusChange")],
    getInitialState: function() {
        var state = store.initState();
            return state;
    },
    _onStatusChange: function(state) {
            this.setState(state);
    },
    getRect: function() {
        var d = $(document);
        return {
            width: d.width() - 195,
            height: d.height() - 40
        }
    },
    componentWillMount:function(){
        store.actions.geturl();
    },
    componentDidMount: function() {
        var self = this;
        this.resetFrame();
        $(window).on("resize", function() {
            self.resetFrame();
        });
        //utils.showLoading();
    },
    resetFrame: function() {
        var rect = this.getRect();
        $("iframe").css({width:rect.width + "px", height:rect.height+"px"});
    },
    render: function() {
        return (
            <div id="monitor">
                <iframe src={this.state.url}></iframe>
            </div>
        );
    }
});


module.exports = Monitor;
