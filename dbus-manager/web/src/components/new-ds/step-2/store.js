var Reflux = require('reflux');
var $ = require('jquery')
var utils = require('../../common/utils');
var scriptGenerator = require('../../common/configScript/config-script-generator');

var actions = Reflux.createActions(['initialLoad','dataSourceSelected',
    'dataSchemaSelected','fillTable','clearTable','nextStep','passParam','closeDialog',
    'openDialog']);
var store = Reflux.createStore({
    //监听所有的actions
    listenables: [actions],
    state: {
        dsOptions: [],//数据源选项
        schemaOpts: [],//schema选项
        description:'',//schema描述信息
        status:'',//schema对应的状态
        src_topic:'',//schema对应的src_topic
        target_topic:'',//schema对应的target_topic
        data: [], //存储从源端查询出的所有的schema信息，["schemaName","status","srcTopic","targetTopic"]
        tableCol:[],//存储所有table信息
        isSelectedTable:[],//存储之前复选框选中的table名字
        defaultSchemOpts:[],//缺省的schema选项
        newSelectedTable:[], //存储新的被选中的table名字
        dialog: {
            show:false,
            content:"",
            identity:""
        },
        dsType:"",
        totalTables:0
    },
    initState: function() {
        return this.state;
    },
    onInitialLoad: function() {
        var self = this;
        self.state.dsOptions = [];
        self.state.schemaOpts = [];
        self.state.tableCol = [];
        self.state.data = [];
        self.state.newSelectedTable = [];
        self.state.isSelectedTable = [];
        self.state.description = '';
        self.state.status = '';
        self.state.src_topic = '';
        self.state.target_topic = '';
        self.state.defaultSchemOpts = '';
        self.state.dialog.show = false;
        self.state.dialog.content = "";
        self.state.dialog.identity = "";
        self.state.dsType = "";
        self.state.totalTables = 0;
        self.trigger(self.state);
    },
    onPassParam: function(params,callback) {
        var self = this;
        var dsOptions = [];
        var defaultSchemOpts = [];
        if(params.dsId)
        {
            dsOptions.push({value:params.dsId,text:[params.dsType, params.dsName].join("/")});
            if(params.schemaName)
            {
                defaultSchemOpts.push({value:params.schemaName, text: params.schemaName});
            }
        }
        else
        {
            alert("dsId is null！");
        }
        self.state.dsType = params.dsType;
        self.state.dsOptions = dsOptions;
        self.state.defaultSchemOpts = defaultSchemOpts;
        self.trigger(self.state);

        if(dsOptions && defaultSchemOpts == '')
        {
            utils.hideLoading();
            self.onDataSourceSelected(params.dsId);
        }
        else if(dsOptions && defaultSchemOpts !== '')
        {
            //获取对应数据源的schema信息。
            $.get(utils.builPath("schema/listByDsName"), {dsName: params.dsName}, function(result) {
                if(result.status !== 200) {
                    if(console)
                        console.error(JSON.stringify(result));
                    alert("Load data schema fail！");
                    return;
                }
                //返回从源库中查询的["schemaName","status","srcTopic","targetTopic"]信息
                self.state.data = result.data;
                self.trigger(self.state);
                utils.hideLoading();
                self.onDataSchemaSelected(params.dsId,params.schemaName,callback);
            });
        }
    },
    onDataSourceSelected: function(dsId) {
        utils.showLoading();
        var self = this;
        var list = self.state.dsOptions;
        var dsName = "";
        //当数据源选中时，清空页面上的其他信息：schema信息、table信息。
        self.state.tableCol = [];
        self.state.schemaOpts = [];
        self.state.status = '';
        self.state.src_topic = '';
        self.state.target_topic = '';
        self.trigger(self.state);
        //value代表dsId,text代表dsType/dsName
        list.forEach(function(e){
            if(e.value == dsId)
            {
                var start = e.text.indexOf("/");
                dsName = e.text.substring(start + 1);
            }
        });
        //获取对应数据源的schema信息。
        $.get(utils.builPath("schema/listByDsName"), {dsName: dsName}, function(result) {
            if(result.status !== 200) {
                if(console)
                    console.error(JSON.stringify(result));
                alert("Load data schema fail！");
                return;
            }
            var list = [];
            result.data.forEach(function(e) {
                   list.push({value:e.schemaName, text: e.schemaName});
                });
            //返回从源库中查询的["schemaName","status","srcTopic","targetTopic"]信息
            self.state.data = result.data;
            //schemaOpts：schema下拉框选项
            self.state.schemaOpts = list;
            self.trigger(self.state);
            utils.hideLoading();
        });
    },
    onFillTable: function(tableName) {
        var self = this;
        var newSelectedTable = self.state.newSelectedTable;
        //存储新选中的table信息，不包含之前已经选中的信息
        newSelectedTable.push({tableName:tableName});
        self.state.newSelectedTable = newSelectedTable;
        self.trigger(self.state);
    },

    onClearTable:function(tableName) {
        var self = this;
        var newSelectedTable = self.state.newSelectedTable;
        //如果table没有被选中，清除newSelectedTable中的table信息。
        newSelectedTable.forEach(function(e){
            if(tableName == e.tableName)
            {
                e.tableName = "";
            }
        });
        self.state.newSelectedTable = newSelectedTable;
        self.trigger(self.state);
    },
    onDataSchemaSelected: function(dsId,schemaName,callback) {
        //当schema选中时，更新schema信息，清空页面上的table信息。
        utils.showLoading();
        var self = this;
        var data = self.state.data;
        var isSelectedTable = [];
        self.state.tableCol = [];
        self.trigger(self.state);
        //如果对应的schema已经插入到管理库中,显示源库中schema信息
        var output_topic = '';
        data.forEach(function(e){
            if(e.schemaName == schemaName)
            {
                output_topic = e.targetTopic;
            }
        });
        //如果对应的schema已经插入到管理库中，显示管理库中schema的信息:["status","srcTopic","targetTopic"]。
        $.get(utils.builPath("schema/checkManagerSchema"),
            {dsId:dsId,schemaName:schemaName}, function(result) {
                if(result.status !== 200)
                {
                    if(console)
                    {
                        console.error(JSON.stringify(result));
                        alert("Check manager library schema fail！");
                        return;
                    }
                }
                if(result.data.length != 0)
                {
                    //管理库中存在对应的schema信息，页面显示管理库中的信息。
                    result.data.forEach(function(e){
                        self.state.status = e.status;
                        self.state.src_topic = e.srcTopic;
                        self.state.target_topic = e.targetTopic;
                        self.state.description = e.description;
                    });
                }
                else{
                //管理库中不存在对应的schema信息，页面显示源库中的信息。
                    console.log("data: " + JSON.stringify(data));
                    data.forEach(function(e){
                        if(e.schemaName == schemaName)
                        {
                            self.state.status = e.status;
                            self.state.src_topic = e.srcTopic;
                            self.state.target_topic = e.targetTopic;
                            self.state.description = '';
                        }
                    });
                }
                callback(self.state.description);
        });
        //获取dsName
        var list = self.state.dsOptions;
        var dsName = "";
        list.forEach(function(e){
            if(e.value == dsId)//用===来判断的话，会出错！
            {
                var start = e.text.indexOf("/");
                dsName = e.text.substring(start + 1);
            }
        });
        ///根据dsId,dsName和schemaName获取表信息。
        $.get(utils.builPath("tableMeta/listTable"), {dsID:dsId,dsName:dsName,schemaName:schemaName},
        function(result) {
            if(result.status !== 200)
            {
                if(console)
                    {
                        console.error(JSON.stringify(result));
                        alert("Load table fail！");
                        return;
                    }
            }
            //将表信息写入tablesCol。
            var tablesCol = [];
            result.data.forEach(function(e) {
                if(e.physicalTableRegex!=='' && e.outputTopic!=='')
                {
                    tablesCol.push({tableName:e.tableName, physicalTableRegex:e.physicalTableRegex,
                    outputTopic:e.outputTopic, incompatibleColumn:e.incompatibleColumn,
                    columnName:e.columnName,__ckbox_checked__:e.__ckbox_checked__,__disabled__:e.__disabled__});
                }
                else if(e.physicalTableRegex!=='' && e.outputTopic == '')
                {
                    tablesCol.push({tableName:e.tableName, physicalTableRegex:e.physicalTableRegex,
                    outputTopic:output_topic, incompatibleColumn:e.incompatibleColumn,
                    columnName:e.columnName,__ckbox_checked__:e.__ckbox_checked__,__disabled__:e.__disabled__});
                }
                else if(e.physicalTableRegex == '' && e.outputTopic!=='' )
                {
                    tablesCol.push({tableName:e.tableName, physicalTableRegex:e.tableName,
                    outputTopic:e.outputTopic, incompatibleColumn:e.incompatibleColumn,
                    columnName:e.columnName,__ckbox_checked__:e.__ckbox_checked__,__disabled__:e.__disabled__});
                }
                else if(e.physicalTableRegex == '' && e.outputTopic == '')
                {
                    tablesCol.push({tableName:e.tableName, physicalTableRegex:e.tableName,
                    outputTopic:output_topic, incompatibleColumn:e.incompatibleColumn,
                    columnName:e.columnName,__ckbox_checked__:e.__ckbox_checked__,__disabled__:e.__disabled__});
                }
            });
            tablesCol.forEach(function(e){
                if(e.__ckbox_checked__)
                    //isSelectedTable存储之前被选中的table
                    isSelectedTable.push({tableName:e.tableName});
            });
            self.state.tableCol = tablesCol;
            self.state.isSelectedTable = isSelectedTable;
            self.trigger(self.state);
            utils.hideLoading();
        });
    },

    //这个函数与新增schema和table的函数不一样
    onNextStep:function(dsId,schemaName,description,callback) {
        var self = this;
        var list = self.state.dsOptions;
        var newSelectedTable = self.state.newSelectedTable;
        var status = self.state.status;
        var src_topic = self.state.src_topic;
        var target_topic = self.state.target_topic;
        var tableCol = self.state.tableCol;
        var sourceTable = [];
        var flag = 0;   //判断是否至少选中一张表
        var tables = [];
        var tableName = '';
        var tablesCount = 0;

        var dsName = "";
        var dsType = "";
        list.forEach(function(e){
            if(e.value == dsId)
            {
                var start = e.text.indexOf("/");
                dsType = e.text.substring(0, start);
                dsName = e.text.substring(start + 1);
            }
        });
        
        newSelectedTable.forEach(function(e) {
            if(e.tableName)
            {
                sourceTable.push({dsName:dsName,schemaName:schemaName,tableName:e.tableName});
            }
        });
        newSelectedTable.forEach(function(e) {
            if("" !== e.tableName)
            {
                tableName = e.tableName;
                tableCol.forEach(function(e) {
                    if(tableName == e.tableName)
                    {
                       tables.push(e);
                    }
                });
                flag = 1;
            }
        });

        if(flag != 1)
        {
            alert("Please select at least one table!");
            callback(flag,tablesCount);
            return;
        }
        var p = {
            dsType:dsType,
            dsName:dsName,
            dsId:dsId,
            schemaName:schemaName,
            description:description,
            status:status,
            src_topic:src_topic,
            target_topic:target_topic,
            tables:tables
        }
        console.log("tables: " + JSON.stringify(tables));
        tablesCount = tables.length;
        if(p.schemaName == 0){
            callback(flag,tablesCount);
            alert("Please select a schema!");
            return;
        }
        $.when($.get(utils.builPath("insertSchemaAndTable/insert"),p), $.get(utils.builPath("insertTablesInSource/insertTable"),{sourceTable})).then(
            function(res1, res2) {
                if(res1[0].status == 200 && res2[0].status == 200)
                {
                    callback(flag,tablesCount);
                }
                else
                {
                    flag = 0;
                    alert("insertSchemaAndTable and insertTablesInSource fail！");
                    callback(flag,tablesCount);
                }
            }, function error(e) {
                alert("onNextStep: " + e.message);
                }
        );
    },
    onOpenDialog:function(schemaName){
        scriptGenerator.createScript(this, schemaName);
    },
    onCloseDialog: function() {
        this.state.dialog.show = false;
        this.trigger(this.state);
    }
});

store.actions = actions;
module.exports = store;
