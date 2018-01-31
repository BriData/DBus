var Reflux = require('reflux');
var React = require('react');
var $ = require('jquery')
var utils = require('../common/utils');

var cells = require('../common/table/cells');
var TextCell = cells.TextCell;
//var Table = require('../common/table/default-table');
var SqlTable =  require('./dbus-data-sqltable');
var Tab = require('fixed-data-table');
var Column = Tab.Column;
var Cell = Tab.Cell;

var actions = Reflux.createActions(['initialLoad','search',
    'openDialogByKey',
    'closeDialogColumn',
    'openDialogSql','closeDialogSql',
    'executeDialogSqlonMaster','executeDialogSqlonSlave']);

var store = Reflux.createStore({
    state: {
        dsOptions: [],
        searchParam:[],
        data: [],
        sqlData:[],
        tableSql:null,
        dialog: {
            showSql: false,
            showColumn:false,
            content:"",
            identity:""
        },
    },
    initState: function() {
        return this.state;
    },
    onInitialLoad: function() {
        var self = this;
        $.get(utils.builPath("ds/list"), {}, function(result) {
            if(result.status !== 200) {
                alert("加载数据源失败");
                return;
            }
            var list = [];
            result.data.forEach(function (e) {
                if (e.dsType == utils.dsType.oracle || e.dsType == utils.dsType.mysql) {
                    list.push({value: e.id, text: [e.dsType, e.dsName].join("/")});
                }
            });
            self.state.dsOptions = list;
            self.state.searchParam = result.data;
            console.log("self.state.searchParam: " + JSON.stringify(self.state.searchParam));
            self.trigger(self.state);
        });
    },
    onOpenDialogByKey: function(key, obj) {
        var content=String(obj[key]);
        content = content.replace(/\n/gm, "<br/>");
        content = content.replace(/[' ']/gm, "&nbsp;");
        this.state.dialog.showColumn = true;
        this.state.dialog.content = content;
        this.state.dialog.identity = key ;
        this.trigger(this.state);
    },
    onOpenDialogSql:function(sql){
        this.state.dialog.showSql = true;
        this.state.tableSql = null;
        this.state.dialog.content = sql;
        this.state.dialog.identity = "sql";
        this.trigger(this.state);
    },
    onCloseDialogColumn: function() {
        this.state.dialog.showColumn = false;
        this.trigger(this.state);
    },
    onCloseDialogSql: function() {
        this.state.dialog.showSql = false;
        this.trigger(this.state);
    },
    onExecuteDialogSqlonSlave:function(sqlParam){
        var self = this;
        var p;
        self.state.searchParam.forEach(function(e){
            if(e.id == sqlParam.dsId){
                p = {
                    dsId:e.id,
                    dsType:e.dsType,
                    URL:e.slaveURL,
                    user:e.dbusUser,
                    password:"",
                    sql:sqlParam.sql
                }
            }
        });
        if(p == null) {
            alert("onExecuteDialogSqlonSlave is failed!");
        }
        console.log("onExecuteDialogSqlonSlave: " + JSON.stringify(p));
        $.get(utils.builPath("tables/executeSql"), p, function(result) {
            if(result.status !== 200) {
                if(console)
                    console.error(JSON.stringify(result));
                utils.hideLoading();
                alert("执行你的sql语句失败");
                return;
            }
            if(result.data == null){
                utils.hideLoading();
                alert("sql语句只能是select语句");
                return;
            }
            self.state.sqlData = result.data;
            self.createTableSql(self.state.sqlData,sqlParam);
            self.trigger(self.state);
        });
    },
    onExecuteDialogSqlonMaster:function(sqlParam){
        var self = this;
        var p;
        self.state.searchParam.forEach(function(e){
            if(e.id == sqlParam.dsId){
                p = {
                    dsId:e.id,
                    dsType:e.dsType,
                    URL:e.masterURL,
                    user:e.dbusUser,
                    password:"",
                    sql:sqlParam.sql
                }
            }
        });

        if(p == null) {
            alert("onExecuteDialogSqlonMaster is failed!");
        }
        console.log("onExecuteDialogSqlonSlave: " + p);

        $.get(utils.builPath("tables/executeSql"), p, function(result) {
            if(result.status !== 200) {
                if(console)
                    console.error(JSON.stringify(result));
                utils.hideLoading();
                alert("执行你的sql语句失败");
                return;
            }
            if(result.data == null){
                utils.hideLoading();
                alert("sql语句只能是select语句");
                return;
            }
            self.state.sqlData = result.data;
            self.createTableSql(self.state.sqlData,sqlParam);
            self.trigger(self.state);
        });
    },
    createTableSql:function(sqlData,sqlParam){
        var self = this;
        var rows = [];
        for(var key in sqlData[0]){
            rows.push( <Column
                header={ <Cell > {key} </Cell> }
                cell={ <TextCell data={sqlData}  col={key}  onDoubleClick={this.onOpenDialogByKey.bind(this,key)} />}
                width={150}
                flexGrow={1}/>);
        }
        self.state.tableSql = [];
        /*
         var tableName = sqlParam.sql.substring(sqlParam.sql.indexOf("from")+4,sqlParam.sql.length);
         self.state.tableSql.push(<h4 className="col-xs-12">{tableName}</h4>);
         */
        self.state.tableSql.push(<SqlTable rowsCount={100}>{rows}</SqlTable>);
        self.trigger(self.state);
        utils.hideLoading();
    },
    //监听所有的actions
    listenables: [actions],
    onSearch: function(dsId){
        var self = this;

        var p;
        self.state.searchParam.forEach(function(e){
            if(e.id == dsId){
                p = {
                    dsId:e.id,
                    dsType:e.dsType,
                    URL:e.masterURL,
                    user:e.dbusUser,
                    password:""
                }
            }
        });
        $.get(utils.builPath("ds/searchFromSource"), p, function(result) {
            if(result.status !== 200) {
                if(console)
                    console.error(JSON.stringify(result));
                alert("加载业务库DBUS Data失败");
                return;
            }
            self.state.data = result.data;
            self.trigger(self.state);
            utils.hideLoading();
        });
    }
});

store.actions = actions;
module.exports = store;
