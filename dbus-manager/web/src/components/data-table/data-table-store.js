var React = require('react');

var utils = require('../common/utils');
const {TextCell} = require('../common/table/cells');
const {Table, Column, Cell} = require('fixed-data-table');
var Select = require('../common/select-encode-algorithms');
var Reflux = require('reflux');
var $ = require('jquery');
var utils = require('../common/utils');

var ReactDOM = require('react-dom');

var actions = Reflux.createActions(['initialLoad','dataSourceSelected','search','closeDialog','handleSubmit','openUpdate','pullWhole','pullIncrement','stop',
    'openDialogByKey',
    'openDialogConfigure',
    'saveConfigure',
    'closeConfigure',
    'closeDialogZK',
    'openVersionDifference',
    'readTableVersion',
    'versionChanged',
    'configRule',
    'confirmStatusChange',
    'independentPullWhole',
    'deleteTable',
    'takeEffect',
    'changeInactive',
    'readOutputTopic',
    'closeReadOutputTopic',
    'encodePackageChange']);

var store = Reflux.createStore({
    state: {
        dsOptions: [],
        searchParam:[],
        data: [],
        currentPageNum: 1,

        dialogSelectPlugin: [],
        dialog: {
            show: false,
            content:"",
            identity:"",

            showZK:false,
            contentZK:"",
            identityZK:"",

            showConfigure: false,
            contentConfigure:"",
            identityConfigure:"",
            encodePackages: [],
            encodeAlgorithms:[],
            tableInformation:{
                tableId:null,
                tableColumnNames:[]
            }
        },
        id:0,
        tableName:"",
        tableVersion:[],
        versionData:"",

        // 读kafka对话框
        showReadOutputTopic: false,
        dialogOutputTopic: null,


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
            result.data.forEach(function(e) {
                list.push({value:e.id, text: [e.dsType, e.dsName].join("/")});
            });
            self.state.dsOptions = list;
            self.trigger(self.state);
            self.state.searchParam = result.data;
            self.onSearch({});
        });
    },
    onDeleteTable: function (param, search) {
        var self = this;
        $.get(utils.builPath("tables/deleteTable"), param, function(result) {
            if(result.status !== 200) {
                alert("Delete table failed");
                return;
            }
            search(null, self.state.currentPageNum);
        });
    },

    onReadOutputTopic: function (obj) {
        var self = this;
        self.state.dialogOutputTopic = obj.outputTopic;
        self.state.showReadOutputTopic = true;
        self.trigger(self.state);
    },
    onCloseReadOutputTopic: function () {
        var self = this;
        self.state.dialogOutputTopic = null;
        self.state.showReadOutputTopic = false;
        self.trigger(self.state);
    },

    onCloseDialog: function() {
        this.state.dialog.show = false;
        this.trigger(this.state);
    },
    onOpenDialogByKey: function(key, obj) {
        var content=String(obj[key]);
        content = content.replace(/\n/gm, "<br/>");
        content = content.replace(/[' ']/gm, "&nbsp;");
        this.state.dialog.show = true;
        this.state.dialog.content = content;
        this.state.dialog.identity = key ;
        this.trigger(this.state);
    },
    onOpenVersionDifference: function(obj, dataTableSelf) {
        var passParam = {
            id: obj.id,
            dsName: obj.dsName,
            schemaName: obj.schemaName,
            tableName: obj.tableName
        };
        dataTableSelf.props.history.pushState({passParam: passParam}, "/data-table/table-version-difference");

    },

    onConfigRule: function(obj, tableSelf) {
        var passParam = {
            tableId: obj.id,
            dsId: obj.dsID,
            dsName: obj.dsName,
            dsType: obj.dsType,
            schemaName: obj.schemaName,
            tableName: obj.tableName
        };
        utils.showLoading();
        tableSelf.props.history.pushState({passParam: passParam}, "/data-table/config-rule");
    },

    onChangeInactive: function(param) {
        var self = this;
        $.get(utils.builPath("tables/updateTable"), param, function(result) {
            if(result.status !== 200) {
                alert("修改表状态失败");
                return;
            }
            alert("成功修改表状态为inactive");
            self.onSearch({});
        });
    },
    onConfirmStatusChange: function (data, currentTarget) {
        var storeSelf = this;
        var param = {
            tableId: data.id
        };
        $.get(utils.builPath("tables/confirmStatusChange"), param, function (result) {
            if (result.status != 200) {
                alert("未能发送确认消息");
                return;
            }
            storeSelf.onSearch({});
        });
    },
    onEncodePackageChange: function(value, rowIndex) {
        const {dialogSelectPlugin} = this.state
        dialogSelectPlugin[rowIndex] = value
        this.trigger(this.state);
    },
    onOpenDialogConfigure: function(obj, dataTableSelf) {
        var self=this;
        this.state.dialog.contentConfigure = "没有返回数据";
        var param= {
            tableId: obj.id
        };
        $.get(utils.builPath("tables/desensitization"), param, function(result) {
            if(result.status == 200) {
                var desensitizationInformations=result.data;
                console.log("脱敏信息:",desensitizationInformations);
                $.get(utils.builPath("tables/fetchTableColumns"), param, function(result){
                    if(result.status == 200) {
                        console.log("源库中表的列信息:",result.data);
                        self.state.dialog.tableInformation.tableId=param.tableId;// 在保存的时候会用到

                        var tableColumns = result.data;

                        var names=[];
                        for(var i=0; i<tableColumns.length;i++) {
                            names.push(tableColumns[i].COLUMN_NAME);
                        }
                        self.state.dialog.tableInformation.tableColumnNames=names;// 在保存的时候会用到

                        $.get(utils.builPath("tables/fetchEncodeAlgorithms"), function(result){
                            if(result.status == 200) {
                                var builtInEncode = result.data
                                $.get(utils.builPath("tables/fetchEncodePlugin"), function(result) {
                                    if(result.status === 200) {
                                        var encodePlugins = result.data
                                        self.state.dialog.encodePackages = self.createEncodePackages(encodePlugins);
                                        self.state.dialog.encodeAlgorithms = self.createEncodeAlgorithms(builtInEncode, encodePlugins);
                                        self.state.dialog.contentConfigure = self.createDesensitizationTableView(desensitizationInformations
                                            ,tableColumns, dataTableSelf);
                                        self.state.dialog.showConfigure = true;
                                        self.state.dialog.identityConfigure = obj["tableName"] + " encode configure" ; // 对话框标题
                                        self.trigger(self.state);
                                    }
                                    utils.hideLoading();
                                })
                            } else {
                                utils.hideLoading();
                            }
                        });
                    }
                    else {
                        utils.hideLoading();
                    }
                });
            }
            else {
                utils.hideLoading();
            }
        });
    },
    createEncodePackages(encodePlugins) {
        // 内置脱敏合集在这里当做-1
        const list = [{text: '不脱敏', value: ''}, {text: '内置脱敏', value: '-1'}];
        encodePlugins.forEach(plugin => {
            list.push({
                text: plugin.name,
                value: plugin.id+''
            })
        })
        return list;
    },

    createEncodeAlgorithms(builtInEncode, encodePlugins) {
        const list = []
        for (let key in builtInEncode) {
            list.push({
                text: builtInEncode[key],
                value: builtInEncode[key],
                pkg: '-1'
            })
        }
        encodePlugins.forEach(plugin => {
            const algorithmString = plugin.encoders
            if (algorithmString && algorithmString !== '') {
                const algorithmList = algorithmString.split(',')
                algorithmList.forEach(algorithm => {
                    list.push({
                        text: algorithm,
                        value: algorithm,
                        pkg: plugin.id+''
                    })
                })
            }
        })
        console.info("构造的脱敏算法",list)
        return list
    },

    createDesensitizationTableView(desensitizationInformations, tableColumns, dataTableSelf) {
        const dialogSelectPlugin = new Array(tableColumns.length)
        for (var i=0;i<tableColumns.length;i++) {
            dialogSelectPlugin[i] = ''
            tableColumns[i].plugin_id='';
            tableColumns[i].encode_type='';
            tableColumns[i].encode_param='';
            tableColumns[i].truncate='1';
            for (var j=0;j<desensitizationInformations.length;j++) {
                if(tableColumns[i].COLUMN_NAME == desensitizationInformations[j].fieldName) {
                    dialogSelectPlugin[i] = desensitizationInformations[j].pluginId ? desensitizationInformations[j].pluginId + '' : '-1'
                    tableColumns[i].plugin_id=dialogSelectPlugin[i];
                    tableColumns[i].encode_type=desensitizationInformations[j].encodeType;
                    tableColumns[i].encode_param=desensitizationInformations[j].encodeParam;
                    tableColumns[i].truncate=desensitizationInformations[j].truncate+'';
                }
            }
        }
        this.state.dialogSelectPlugin = dialogSelectPlugin
        this.trigger(this.state);

        for(var i=0;i<tableColumns.length;i++) {
            if(tableColumns[i].IS_PRIMARY == "YES") {
                tableColumns[i].COLUMN_NAME = tableColumns[i].COLUMN_NAME + " PK";
            }
        }

        const filterPackage = (pluginId, algorithms) => {
            return algorithms.filter(a => a.pkg === pluginId)
        }

        return (<Table
            overflowX={'auto'}
            overflowY={'auto'}
            rowsCount={tableColumns.length}
            rowHeight={30}
            headerHeight={20}
            width={880}
            height={30*(tableColumns.length+1)}>
            <Column
                header={<cell>Column Name</cell>}
                cell={<TextCell data={tableColumns} col="COLUMN_NAME" onDoubleClick={dataTableSelf.openDialogByKey.bind(this,"COLUMN_NAME")}/>}
                width={130}
            />
            <Column
                header={<cell>Type</cell>}
                cell={<TextCell data={tableColumns} col="DATA_TYPE" onDoubleClick={dataTableSelf.openDialogByKey.bind(this,"DATA_TYPE")}/>}
                width={130}
            />
            <Column
                header={<cell>Encode Package</cell>}
                cell={props => {
                    return (<Select
                            onChange={value => dataTableSelf.handleEncodePackageChange(value, props.rowIndex)}
                            style={{minWidth: "120px"}}
                            className={"desensitization_"+dataTableSelf.state.dialog.tableInformation.tableColumnNames[props.rowIndex]}
                            defaultOpt={tableColumns[props.rowIndex].plugin_id}
                            options={dataTableSelf.state.dialog.encodePackages}
                        />
                    )}}
                width={130}
            />
            <Column
                header={<cell>Encode Type</cell>}
                cell={props => {
                return (<Select
                        style={{minWidth: "120px"}}
                        className={"desensitization_"+dataTableSelf.state.dialog.tableInformation.tableColumnNames[props.rowIndex]}
                        defaultOpt={tableColumns[props.rowIndex].encode_type}
                        options={filterPackage(dataTableSelf.state.dialogSelectPlugin[props.rowIndex], dataTableSelf.state.dialog.encodeAlgorithms)}
                        />
                )}}
                width={130}
            />
            <Column
                header={<cell>Encode Param</cell>}
                cell={props => (
                    <textarea className={"form-control desensitization_"+dataTableSelf.state.dialog.tableInformation.tableColumnNames[props.rowIndex]}  rows="1" cols="48" style={{resize:"none"}}>{tableColumns[props.rowIndex].encode_param}</textarea>
                )}
                width={320}
            />
            <Column
                header={<cell>Trunc</cell>}
                cell={props => (
                    <Select
                    style={{minWidth: "30px"}}
                    className={"desensitization_"+dataTableSelf.state.dialog.tableInformation.tableColumnNames[props.rowIndex]}
                    defaultOpt={tableColumns[props.rowIndex].truncate}
                    options={[{text:"是",value:"1"},{text:"否",value:"0"}]}
                    />
                )}
                width={40}
            />
        </Table>);
    },
    onSaveConfigure: function() {
        utils.showLoading();
        var self=this;
        var param= {
            tableId: self.state.dialog.tableInformation.tableId
        };

        $.get(utils.builPath("tables/desensitization"), param, function(result) {
            if(result.status == 200) {
                var desensitizationInformations=result.data;
                var tableId = self.state.dialog.tableInformation.tableId;
                var names = self.state.dialog.tableInformation.tableColumnNames;
                var param = {};
                var operationCount = 0;
                for(var i=0; i<names.length; i++) {
                    var row=$(".desensitization_"+names[i]);
                    var foundInDesensitizationInformations = false;
                    for(var j=0; j<desensitizationInformations.length; j++) {
                        //分四类讨论
                        if(names[i] == desensitizationInformations[j].fieldName) {
                            foundInDesensitizationInformations = true;
                            //数据库有脱敏信息,弹框中有脱敏信息,执行更新操作
                            if(row[0].value.trim()!='') {
                                var rowParam={
                                    sql_type: 'update',
                                    id: desensitizationInformations[j].id,
                                    plugin_id: row[0].value === '-1' ? undefined : row[0].value,
                                    encode_type: row[1].value,
                                    encode_param: row[2].value,
                                    truncate: row[3].value,
                                    update_time: (new Date()).valueOf()
                                    };
                                operationCount++;
                                param["ruleOperation"+operationCount] = rowParam;
                                break;
                            }
                            //数据库有脱敏信息,弹框中无脱敏信息,执行删除操作
                            else {
                                var rowParam={
                                    sql_type: 'delete',
                                    id: desensitizationInformations[j].id
                                };
                                operationCount++;
                                param["ruleOperation"+operationCount] = rowParam;
                                break;
                            }
                        }
                    }
                    //数据库无脱敏信息,弹框中有脱敏信息,执行添加操作
                    if(!foundInDesensitizationInformations && row[0].value.trim()!='') {
                        var rowParam={
                            sql_type: 'insert',
                            table_id: tableId,
                            field_name: names[i],
                            plugin_id: row[0].value === '-1' ? undefined : row[0].value,
                            encode_type: row[1].value,
                            encode_param: row[2].value,
                            truncate: row[3].value,
                            update_time: (new Date()).valueOf()
                        };
                        operationCount++;
                        param["ruleOperation"+operationCount] = rowParam;
                    }
                    //数据库无脱敏信息,弹框中无脱敏信息,无任何操作
                }
                $.get(utils.builPath("tables/changeDesensitization"), param, function(result) {
                    utils.hideLoading();
                    if(result.status == 200) {
                        alert("配置脱敏信息成功");
                    }
                    else {
                        alert("配置脱敏信息失败");
                    }

                });
            }
        });
    },
    onCloseConfigure: function() {
        this.state.dialog.showConfigure = false;
        this.trigger(this.state);
    },
    onCloseDialogZK:function(){
        this.state.dialog.showZK = false;
        this.trigger(this.state);
    },
    onStop:function(stopParam){
        var self = this;
        $.get(utils.builPath("tables/stop"), stopParam ,function(result) {
            if(result.status !== 200) {
                if(console)
                    console.error(JSON.stringify(result));
                alert("停止失败");
                return;
            }
            self.state.data.list.forEach(function(e){
                if(e.id == stopParam.id){
                    e.status = "abort";
                }
            });
            self.trigger(self.state);
            //self.onSearch({});

        });

    },
    onHandleSubmit:function(formdata){
        var self = this;
        formdata.id =  self.state.id;
        $.get(utils.builPath("tables/updateTable"), formdata, function(result) {
            if(result.status !== 200) {
                alert("修改table失败");
                return;
            }
            self.trigger(self.state);
        });
    },
    onOpenUpdate:function(updateParam){
        var self = this;
        self.state.id = updateParam.id;
        self.state.tableName = updateParam.tableName;
    },

    _reloadCount: 0,
    onTakeEffect: function (obj) {
        var self = this;

        var ctrlTopic = obj.ctrlTopic;
        var ds = obj.dsID;
        var date = new Date();
        var dsName = obj.dsName;
        var dsType = obj.dsType;
        var typeList;
        if (obj.dsType == utils.dsType.mysql) {
            typeList = ["EXTRACTOR_RELOAD_CONF",
                "DISPATCHER_RELOAD_CONFIG",
                "APPENDER_RELOAD_CONFIG",
                "FULL_DATA_PULL_RELOAD_CONF",
                "HEARTBEAT_RELOAD_CONFIG"];
            self._reloadCount = typeList.length;
        }
        else if (obj.dsType == utils.dsType.oracle) {
            typeList = ["DISPATCHER_RELOAD_CONFIG",
                "APPENDER_RELOAD_CONFIG",
                "FULL_DATA_PULL_RELOAD_CONF",
                "HEARTBEAT_RELOAD_CONFIG"];
            self._reloadCount = typeList.length;
        } else if (obj.dsType.startsWith("log_")) {
            typeList = ["LOG_PROCESSOR_RELOAD_CONFIG",
                "HEARTBEAT_RELOAD_CONFIG"];
            self._reloadCount = typeList.length;
        } else {
            alert("Unsupported data source");
            return;
        }

        typeList.forEach(function (type) {
            var message = self._buildReloadMessage(date, type, dsName, dsType);
            self._takeEffect(ds, ctrlTopic, message);
        })
    },

    _buildReloadMessage(date, type, dsName, dsType) {
        return {
            from: "dbus-web",
            id: date.getTime(),
            payload: {
                dsName: dsName,
                dsType: dsType
            },
            timestamp: date.format("yyyy-MM-dd hh:ss:mm:S"),
            type: type
        };
    },


    _takeEffect: function(ds,ctrlTopic,message) {
        var self = this;
        console.log("onSendMessage: " + JSON.stringify(message));
        var param = {
            ds: ds,
            ctrlTopic: ctrlTopic,
            message: message
        };
        $.ajax({
            type: 'GET',
            url: utils.builPath('ctlMessage/send'),
            timeout: 30000,
            data: param,
            success: function(result) {
                if(result.status === 200) {
                    self._reloadCount--;
                    if (self._reloadCount === 0) {
                        alert("成功生效！");
                    }
                } else {
                    alert(message.type + " 消息发送失败！");
                }
            },
            error: function (xml, errType, e) {
                alert(message.type + " 消息发送失败！");
            }
        });
    },

    onIndependentPullWhole:function(typeParam, data){
        var self = this;
        $.get(utils.builPath("fullPull"), typeParam ,function(result){
            var t = result.data.find(function(t) {
                if (t.text === typeParam.messageType) {
                    return t.template;
                }
            });
            if(t == null) return;
            var date = typeParam.date;
            t.template.payload.SEQNO = date.getTime()+'';
            t.template.payload.SPLIT_COL = data.fullpullCol || "";
            t.template.payload.SPLIT_SHARD_SIZE = data.fullpullSplitShardSize || "";
            t.template.payload.SPLIT_SHARD_STYLE = data.fullpullSplitStyle || "";
            var message = utils.extends(t.template, {
                id: date.getTime(),
                timestamp: date.format('yyyy-MM-dd hh:mm:ss.S')
            });
            self._sendIndependentPullWholeMessage(typeParam,data, message);
        });
    },
    _sendIndependentPullWholeMessage: function(typeParam,data, message) {
        var ctrlTopic = typeParam.ctrlTopic;
        var outputTopic = typeParam.outputTopic;
        var strJson = JSON.stringify(message);
        var param = {
            id: message.id,
            type: 'indepent',
            dsName: data.dsName,
            schemaName: data.schemaName,
            tableName: data.tableName,
            ctrlTopic: ctrlTopic,
            tableOutputTopic: data.outputTopic,
            outputTopic: outputTopic,
            message: strJson
        };
        $.post(utils.builPath('fullPull/send'), param , function(result) {
            if(result.status === 200) {
                alert("消息发送成功！");
            } else {
                alert("消息发送失败！");
            }
        });
    },

    onPullWhole:function(pullParam){
        var self = this;
        $.get(utils.builPath("tables/pullWhole"), pullParam, function(result) {
            if(result.status !== 200) {
                alert("发起拉全量请求失败");
                return;
            }
            self.trigger(self.state);
            alert("发起拉全量请求成功");
            //self.onSearch({});
        });

    },
    onPullIncrement:function(pullParam){
        var self = this;
        $.get(utils.builPath("tables/pullIncrement"), pullParam, function(result) {
            if(result.status !== 200) {
                alert("发起拉增量请求失败");
                return;
            }
            self.state.data.list.forEach(function(e){
                if(e.id == pullParam.id){
                    e.status = "ok";
                }
            });
            self.trigger(self.state);
            alert("发起拉增量请求成功");
            //self.onSearch({});
        });

    },
    //监听所有的actions
    listenables: [actions],
    onReadTableVersion: function(versionParam) {
        var self = this;
        //self.trigger(self.state);
        $.get(utils.builPath("tables/readTableVersion"), versionParam, function(result) {
            if(result.status !== 200) {
                alert("zk节点不存在！");
                return;
            }
            var list = [];
            result.data.forEach(function(e) {
                list.push({value:e, text:e});
            });
            list.sort(function (a, b) {
                a = String(a.value);
                b = String(b.value);
                if (a == b) return 0;
                return a > b ? -1 : 1;
            });
            self.state.tableVersion = list;
            self.state.dialog.identityZK = "/DBus/FullPuller/" + versionParam.dsName + "/" + versionParam.schemaName + "/" + versionParam.tableName;
            self.state.dialog.showZK = true;
            self.trigger(self.state);
        });
    },
    onVersionChanged:function(dataParam,obj){
        var self = this;
         $.get(utils.builPath("tables/readVersionData"), dataParam, function(result) {
            if(result.status !== 200) {
                alert("读取zk节点失败");
                return;
            }
            self.state.versionData = result.data;
            self.trigger(self.state);
            ReactDOM.findDOMNode(obj.refs.zkResult).value = self.state.versionData;
        });
    },
    onSearch: function(p){
        var self = this;
        var param = {
            dsID: p.dsID,
            schemaName: p.schemaName,
            tableName: p.tableName,
            pageSize: utils.getFixedDataTablePageSize(),
            pageNum: p.pageNum
        };
        $.get(utils.builPath("tables/search"), param, function(result) {
            self.state.currentPageNum = param.pageNum;
            if(result.status !== 200) {
                if(console)
                    console.error(JSON.stringify(result));
                alert("Load Table failed!");
                return;
            }
            //console.log("result.data: " + JSON.stringify(result.data));
            //if(console.log("onsearch method called"));
            result.data.list.forEach(function(e) {
                var flag = false;
                if(e.tableName == e.physicalTableRegex)
                {
                    flag = true;
                }
                var namespace = '';
                if(flag)
                {
                    namespace = e.dsType + "." + e.dsName + "." + e.schemaName + "." + e.tableName +
                        "." + e.version + "." + "0" + "." + "0";
                }
                else
                {
                    namespace = e.dsType + "." + e.dsName + "." + e.schemaName + "." + e.tableName +
                        "." + e.version + "." + "0" + "." + e.physicalTableRegex;
                }
                e["namespace"] = namespace;
                if (e.verChangeNoticeFlg == 1 && e.status == "ok") e.showStatusHyperlink = true;
                else e.showStatusHyperlink = false;
            });

            self.state.data = result.data;
            self.trigger(self.state);
            utils.hideLoading();
        });
    }
});

store.actions = actions;
module.exports = store;
