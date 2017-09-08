var Reflux = require('reflux');

var actions = Reflux.createActions(['getItems', 'hashChanged']);

var sidebarList = [
    {
        id: 0,
        icon: "glyphicon glyphicon-tree-deciduous",
        text: "Datasource",
        href: "/",
        isActive: false
    },
    {
        id: 1,
        icon: "glyphicon glyphicon-flag",
        text: "Data Schema",
        href: "/data-schema",
        isActive: false
    },
    {
        id: 2,
        icon: "glyphicon glyphicon-hdd",
        text: "Data Table",
        href: "/data-table",
        isActive: false
    },
    {
        id: 4,
        icon: "glyphicon glyphicon-plus",
        text: "New DataLine",
        href: "/nds-step",
        isActive: false
    },

    {
        id: 6,
        icon: "glyphicon glyphicon-arrow-right",
        text: "control message",
        href: "/controlMessage",
        isActive: false
    },
    {
        id: 8,
        icon: "glyphicon glyphicon-sort-by-attributes",
        text: "ZK Manager",
        href: "/zk-manager",
        isActive: false
    },
    {
        id: 9,
        icon: "glyphicon glyphicon-tower",
        text: "Dbus Data",
        href: "/dbus-data",
        isActive: false
    },
    {
        id: 999,
        icon: "glyphicon glyphicon-flash",
        text: "monitor",
        href: "/monitor",
        isActive: false
    },
    {
        id: 10,
        icon: "glyphicon glyphicon-circle-arrow-down",
        text: "full-pull",
        href: "/full-pull",
        isActive: false
    }
];
var debugSidebarList = sidebarList.concat([
    {
        id: 7,
        icon: "glyphicon glyphicon-folder-open",
        text: "Jar manager",
        href: "/jar-mgr",
        isActive: false
    }, {
        id: 5,
        icon: "glyphicon glyphicon-th",
        text: "Topology Manager",
        href: "/topology",
        isActive: false
    }, {
        id: 3,
        icon: "glyphicon glyphicon-star",
        text: "Avro Schema",
        href: "/avro",
        isActive: false
    }
]);

var store = Reflux.createStore({
    items:sidebarList,
    //监听所有的actions
    listenables: [actions],
    //on开头的都是action触发后的回调函数
    getItems () {
        if(global.isDebug) {
            this.items = debugSidebarList;
        }
        return this.items;
    },
    onHashChanged(hash) {
        var items = this.getItems();
        if(hash == "/") {
            for(var i = 1; i < items.length; i++) {
                items[i].isActive = false;
            }
            items[0].isActive = true;
        } else {
            var found = false;
            for(var i = items.length -1; i >=0; i--) {
                if(hash.indexOf(items[i].href) != 0) {
                    items[i].isActive = false;
                } else if(!found){
                    found = true;
                    items[i].isActive = true;
                } else {
                    items[i].isActive = false;
                }
            }
        }

        this.trigger({items:items});
    }
});
store.actions = actions;
module.exports = store;
