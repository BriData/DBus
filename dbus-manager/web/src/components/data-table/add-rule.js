
var React = require('react');
var ReactDOM = require('react-dom');
var antd = require('antd');
var $ = require('jquery');
var utils = require('../common/utils');

var AddRule = React.createClass({

    getInitialState: function () {
        return {
            resultSource: null,
            resultColumns: null,
            scrollx: true,

            kafkaLog:null,
            plainLog:null,
            currentPage: 1,
            dataSource: [],
            columns: [{
                title: "No",
                key: "key",
                render: (text) => (text.key + 1)
            }, {
                title: "Type",
                key: "type",
                render: (text) => (
                            <antd.Select defaultValue={text.type} style={{ width: "100%" }} onChange={this.onSelectChange.bind(this, text.key, "type")}>
                                <antd.Select.Option value="choosein">Choose In</antd.Select.Option>
                                <antd.Select.Option value="chooseout">Choose Out</antd.Select.Option>
                                <antd.Select.Option value="filterin">Filter In</antd.Select.Option>
                                <antd.Select.Option value="filterout">Filter Out</antd.Select.Option>
                                <antd.Select.Option value="split">Split</antd.Select.Option>
                                <antd.Select.Option value="substring">Sub String</antd.Select.Option>
                                <antd.Select.Option value="sql">SQL</antd.Select.Option>
                            </antd.Select>
                )
            }, {
                title: "Columns to be operated Or SQL",
                key: "column",
                render: (text) => (
                    <antd.Input onBlur={this.onBlur.bind(this, text.key, "column")}
                                placeholder="Input columns number or sql here"
                                defaultValue={text.column}/>
                )
            }, {
                title: "Regular Expression",
                key: "re",
                render: (text) => (
                    ["choosein","chooseout","sql"].indexOf(text.type) != -1 ?
                        <div></div>
                        :
                        <antd.Input onBlur={this.onBlur.bind(this, text.key, "re")}
                                placeholder="Input Reg here"
                                defaultValue={text.re}/>
                )
            }, {
                title: "Opetation",
                key: "operation",
                width: 90,
                render: (text) => (
                    <span>
                        <antd.Button title="Run to this rule" icon="step-forward" size="middle" onClick={() => this.runToHere(text.key)}></antd.Button>
                        <span className="ant-divider" />
                        <antd.Button title="Delete" icon="close-circle" size="middle" onClick={() => this.onDelete(text.key)}></antd.Button>
                    </span>
                )
            }]
        }
    },

    componentWillMount: function () {
        var self = this;
        var param = {
            id: this.props.location.state.passParam.id
        };
        $.ajax({
            type: "get",
            url: utils.builPath("tables/searchRules"),
            data: param,
            dataType: "json",
            async: true,
            success: function (data) {
                if (data.status != 200) {
                    alert("获取规则信息失败");
                    utils.hideLoading();
                    return;
                }
                data = data.data;
                var dataSource = [];
                data.forEach(function (line) {
                    dataSource.push({
                        type: line.type,
                        column: line.column,
                        re: line.re
                    });
                });

                for (var i = 0; i < dataSource.length; i++) {
                    dataSource[i].key = i;
                }

                var param = {
                    topic : self.props.location.state.passParam.topic
                };

                $.ajax({
                    type: "get",
                    url: utils.builPath("tables/readKafkaTopic"),
                    data: param,
                    dataType: "json",
                    async: true,
                    success: function (data) {
                        if (data.status != 200) {
                            alert("获取kafka topic失败");
                            utils.hideLoading();
                            return;
                        }
                        data = data.data;
                        console.info("读取到kafka topic",data);
                        self.setState({
                            kafkaLog: data,
                            dataSource: dataSource
                        });
                        utils.hideLoading();
                    },
                    error: function() {
                        alert("读取kafka topic网络连接失败");
                        utils.hideLoading();
                    }
                });
            },
            error: function() {
                alert("网络连接失败");
                utils.hideLoading();
            }
        });
    },

    onSelectChange: function(key, mapKey, event) {
        var dataSource = this.state.dataSource;
        dataSource[key][mapKey] = event;
        this.setState({dataSource: dataSource});
    },

    onBlur: function (key, mapKey, event) {
        var dataSource = this.state.dataSource;
        dataSource[key][mapKey] = event.target.value;
        this.setState({dataSource: dataSource});
    },

    onDelete: function (key) {
        var dataSource = this.state.dataSource;
        dataSource.splice(key, 1);
        for (var i = 0; i < dataSource.length; i++) {
            dataSource[i].key = i;
        }
        this.setState({dataSource: dataSource});
    },

    addRule: function () {
        var dataSource = this.state.dataSource;
        dataSource.push({
            type: "choosein",
            column: "1",
            re: ""
        });
        for (var i = 0; i < dataSource.length; i++) {
            dataSource[i].key = i;
        }
        this.setState({
            dataSource: dataSource,
            currentPage:(parseInt((dataSource.length-1)/4)+1)
        });
    },

    saveAllRules: function() {

        utils.showLoading();
        var tableId = this.props.location.state.passParam.id;
        var dataSource = this.state.dataSource;
        var rules = [];

        for(var i=0;i<dataSource.length ; i++) {
            rules.push({
                tableId: tableId,
                order: i+1,
                type: dataSource[i].type,
                column: dataSource[i].column,
                re: dataSource[i].re
            });
        }
        var param = {
            tableId : tableId,
            rules : rules
        };
        $.ajax({
            type: "get",
            url: utils.builPath("tables/saveRules"),
            data: param,
            dataType: "json",
            async: true,
            success: function (data) {
                if (data.status != 200) {
                    utils.hideLoading();
                    alert("保存失败");
                    return;
                }

                utils.hideLoading();
                alert("保存成功");

            },
            error: function() {
                utils.hideLoading();
                alert("网络连接失败");
            }
        });
    },

    runAllRules: function() {
        this.runToHere(this.state.dataSource.length - 1);
    },

    runToHere: function(last) {
        var plainLog = this.refs.plainLog.value;
        var result = this.initResultTable(plainLog);
        var rules = this.state.dataSource;

        for(var i=0;i<=last;i++) {
            if(rules[i].type == "choosein") {
                result = this.chooseInRule(result, rules[i].column);
            } else if(rules[i].type == "chooseout") {
                result = this.chooseOutRule(result, rules[i].column);
            } else if(rules[i].type == "filterin") {
                result = this.filterInRule(result, rules[i].column, rules[i].re);
            } else if(rules[i].type == "filterout") {
                result = this.filterOutRule(result, rules[i].column, rules[i].re);
            } else if(rules[i].type == "split") {
                result = this.splitRule(result, rules[i].column, rules[i].re);
            } else if(rules[i].type == "substring") {
                result = this.subStringRule(result, rules[i].column, rules[i].re);
            } else if(rules[i].type == "sql") {
                result = this.sqlRule(result, rules[i].column, rules[i].re);
            }
        }

        var resultSource = this.transformSource(result);
        var maxCount = this.getMaximunColumnsCount(result);
        var resultColumns = this.transformColumn(maxCount);
        this.setState({
            resultSource: resultSource,
            resultColumns: resultColumns,
            scrollx: maxCount*100
        });
        //utils.hideLoading();
    },

    getFormattedColumnsFromInput(column) {
        var result = [];
        var columns = column.split(",");
        columns.map(function(oneColumn){
            var numbers = oneColumn.split("-");
            if(numbers.length == 1) {
                result.push(parseInt(numbers[0])-1);
            } else if(numbers.length == 2) {
                for(var i=parseInt(numbers[0]);i<=parseInt(numbers[1]);i++) {
                    result.push(i-1);
                }
            }
        });
        return result;
    },

    chooseInRule: function (data, column) {
        var columns = this.getFormattedColumnsFromInput(column);
        var result = [];
        data.map(function(lineData){
            var newLineData = [];
            columns.map(function(number){
                newLineData.push(lineData[number]);
            });
            result.push(newLineData);
        });
        return result;
    },

    chooseOutRule: function (data, column) {
        var columns = this.getFormattedColumnsFromInput(column);
        var result = [];
        data.map(function(lineData){
            var newLineData = [];
            var p = 0;
            for(var i=0;i<lineData.length;i++) {
                if(p<columns.length && i==columns[p]) {
                    p++;
                } else {
                    newLineData.push(lineData[i]);
                }
            }
            result.push(newLineData);
        });
        return result;
    },

    filterInRule: function(data, column, re) {
        var columns = this.getFormattedColumnsFromInput(column);
        var result = [];
        re = eval('/'+ re +'/');
        data.map(function(lineData){
            var flag = false;
            columns.map(function(number){
                if(lineData[number] && lineData[number].match(re)) flag = true;
            });
            if(flag) result.push(lineData);
        });
        return result;
    },

    filterOutRule: function(data, column, re) {
        var columns = this.getFormattedColumnsFromInput(column);
        var result = [];
        re = eval('/'+ re +'/');
        data.map(function(lineData){
            var flag = false;
            columns.map(function(number){
                if(lineData[number] && lineData[number].match(re)) flag = true;
            });
            if(!flag) result.push(lineData);
        });
        return result;
    },

    splitRule: function(data, column, re) {
        var columns = this.getFormattedColumnsFromInput(column);
        var result = [];
        re = eval('/'+ re +'/');
        data.map(function(lineData){
            var newLineData = [];
            var p = 0;
            for(var i=0;i<lineData.length;i++) {
                if(p<columns.length && i==columns[p]) {
                    if(lineData[i]) {
                        lineData[i].split(re).map(function(str){
                            newLineData.push(str);
                        });
                    } else {
                        newLineData.push(lineData[i]);
                    }
                    p++;
                } else {
                    newLineData.push(lineData[i]);
                }
            }
            result.push(newLineData);
        });
        return result;
    },

    subStringRule: function(data, column, re) {
        var columns = this.getFormattedColumnsFromInput(column);
        var result = [];
        re = re.split(",");
        data.map(function(lineData){
            var newLineData = [];
            var p = 0;
            for(var i=0;i<lineData.length;i++) {
                if(p<columns.length && i==columns[p]) {
                    if(lineData[i]) {
                        if(re.length == 1) {
                            newLineData.push(lineData[i].substring(parseInt(re[0])-1));
                        } else if (re.length == 2) {
                            newLineData.push(lineData[i].substring(parseInt(re[0])-1,parseInt(re[1])));
                        }
                    } else {
                        newLineData.push(lineData[i]);
                    }
                    p++;
                } else {
                    newLineData.push(lineData[i]);
                }
            }
            result.push(newLineData);
        });
        return result;
    },

    sqlRule: function(data, column, re) {
        if (!data) return null;
        var param = {
            sql: column,
            length: data.length,
            data: data
        }
        var canReturn = false;
        var returnData = undefined;
        $.ajax({
            type: "post",
            url: utils.builPath("tables/executeSqlRule"),
            data: param,
            dataType: "json",
            async: false,
            success: function (data) {
                if (data.status != 200) {
                    alert("执行SQL语句失败");
                    canReturn = true;
                } else {
                    returnData = data.data;
                    canReturn = true;
                }
            },
            error: function(xhr,status,error) {
                alert("无法执行SQL语句");
                canReturn = true;
            }
        });
        for(;;)
        {
            if(canReturn){
                return returnData;
            }
        }
    },

    getMaximunColumnsCount: function(result) {
        var length=0;
        if (result) {
            for (var i = 0; i < result.length; i++) {
                if (result[i].length > length) length = result[i].length;
            }
        }
        return length;
    },
    initResultTable: function(plainLog) {
        var result = [];
        var tableLog = plainLog.split("\n");
        for(var i=0;i<tableLog.length;i++) {
            if(tableLog[i] == "") continue;
            result.push([tableLog[i]]);
        }
        return result;
    },

    transformSource: function(result) {
        var source=[];
        if (result) {
            for (var i = 0; i < result.length; i++) {
                var item = {key: i};
                for (var j = 0; j < result[i].length; j++) {
                    item["col_" + j] = result[i][j];
                }
                source.push(item);
            }
        }
        return source;
    },

    transformColumn: function(length) {
        var columns=[];
        for(var i=0;i<length;i++) {
            var item={
                title: i+1,
                dataIndex: "col_"+i,
                key: "col_"+i,
                width: 100
            };
            columns.push(item);
        }
        return columns;
    },
    onPlainLogSelectChange: function(value) {
        var plainLog = "";
        var kafkaLog = this.state.kafkaLog;
        for(var i=(kafkaLog.length-value >= 0 ? kafkaLog.length-value : 0);i<kafkaLog.length;i++) {
            plainLog = plainLog + kafkaLog[i]+"\n";
        }
        this.refs.plainLog.value=plainLog;
    },

    render: function () {
        return (
            <div className="container-fluid">
                <div className="row header">
                    <h4 className="col-xs-6">Plain Log:</h4>
                    <h4 className="col-xs-3"><div style={{float:"right"}}>kafka Log Count:</div></h4>
                    <div className="col-xs-3">
                    <antd.Select style={{width:"100%"}} onChange={this.onPlainLogSelectChange}>
                        <antd.Select.Option value="10">Recent 10</antd.Select.Option>
                        <antd.Select.Option value="50">Recent 50</antd.Select.Option>
                        <antd.Select.Option value="100">Recent 100</antd.Select.Option>
                        <antd.Select.Option value="500">Recent 500</antd.Select.Option>
                        <antd.Select.Option value="1000">Recent 1000</antd.Select.Option>
                    </antd.Select>
                    </div>

                    <div className="col-xs-12"><textarea className="form-control" ref="plainLog" placeholder="Please input log here" rows="2"
                                                         style={{width:"100%"}}></textarea></div>
                    <h4 className="col-xs-8">Rules:</h4>
                    <div className="col-xs-4">
                        <div style={{float:"right"}}>
                            <antd.Button icon="save" type="primary" onClick={this.saveAllRules}>Save all rules</antd.Button>
                            <span className="ant-divider" />
                            <antd.Button icon="caret-right" onClick={this.runAllRules}>Run all rules</antd.Button>
                            <span className="ant-divider" />
                            <antd.Button icon="plus-circle" onClick={this.addRule}>Add</antd.Button>
                        </div>
                    </div>
                    <div style={{overflowX:"auto",overflowY:"auto",width:"100%"}} className="col-xs-12">
                        <antd.Table ref="tableSource" locale={{emptyText:"No rules"}} bordered size="default"
                                    pagination={{
                                    current:this.state.currentPage,
                                    pageSize: 4,
                                    size: "",
                                    onChange: (page, pageSize)=> { this.setState({currentPage:page}) }
                                    }}
                                    dataSource={this.state.dataSource}
                                    columns={this.state.columns}></antd.Table>


                    </div>
                    <h4 className="col-xs-8">Result:</h4>
                    <div style={{overflowX:"auto",overflowY:"auto",width:"100%"}} className="col-xs-12">
                        <antd.Table locale={{emptyText:"No results"}}
                                    pagination={false}
                                    scroll={{x:this.state.scrollx,y:600}}
                                    bordered
                                    size="default"
                                    dataSource={this.state.resultSource}
                                    columns={this.state.resultColumns}>
                        </antd.Table>
                    </div>

                </div>
            </div>
        );
    }
});

module.exports = AddRule;
