var React = require('react');
var Reflux = require('reflux');
var SidebarItem = require('./sidebar-item');
var store = require('./sidebar-store');

var Sidebar = React.createClass({
    mixins: [Reflux.listenTo(store,"onStatusChange")],
    getInitialState: function() {
        return {
            items: store.getItems()
        };
    },
    click: function(idx) {
        //store.actions.click(idx);
    },
    componentWillMount: function() {
        $(function(e){
            var self = this;
            var hash = document.location.hash;
            var begin = hash.indexOf("/");
            var end = hash.indexOf("?");
            hash = hash.substring(begin, end);
            store.actions.hashChanged(hash);
        });
    },
    componentDidMount: function() {
        $(window).on("hashchange", function(e){
            var self = this;
            var hash = document.location.hash;
            var begin = hash.indexOf("/");
            var end = hash.indexOf("?");
            hash = hash.substring(begin, end);
            store.actions.hashChanged(hash);
        });
    },
    onStatusChange: function(state) {
        this.setState(state);
    },
    render: function() {
        var self = this;
        var itemList = [];
        var key = 0;
        this.state.items.forEach(function(item){
            // 没有手动的添加unique constant key的话，react无法记录dom操作
            itemList.push(
                <SidebarItem key={++key} item={item} callback={ self.click }/>
            );
        });
        return (
            <SidebarNav>
                {itemList}
            </SidebarNav>
        );
    }
});

var SidebarNav = React.createClass({
    render: function() {
        return (
            <div id="sidebar-nav">
                <ul id="dashboard-menu">
                    {this.props.children}
                </ul>
            </div>
        )
    }
});

module.exports = Sidebar;
