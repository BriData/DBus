var React = require('react');
var ReactDOM = require('react-dom');
var B = require('react-bootstrap');
var Select = require('../common/select');
var TF = require('../common/table/tab-frame');
var $ = require('jquery');
var utils = require('../common/utils');
const {ComparisonTable} = require('../common/table/custom-cells');

var TableVersionDifference = React.createClass({
    getInitialState: function () {
        return {
            content: null,
            versionOptions: null,
            version1: null,
            version2: null,
            defaultValueSelect1: null,
            defaultValueSelect2: null
        }
    },

    componentWillMount: function () {
        this.getVersionIdList();
    },

    getVersionIdList: function () {
        const self = this;
        var param = {
            tableId: this.props.location.state.passParam.id
        };

        $.get(utils.builPath("tables/getVersionListByTableId"), param, function(data) {
            utils.hideLoading();
            if (data.status != 200) {
                alert("获取表版本信息失败");
                return;
            }
            data = data.data;
            var result = [];
            data.forEach(function (line) {
                result.push({
                    value: line.id,
                    text: "Id:" + line.id + ", Ver:" + line.version + ", InVer:" + line.innerVersion + ", Time:" + line.updateTime
                });
            });

            if(result.length >= 2) {
                self.getComparisonResult(result[1].value, result[0].value);
                self.setState({
                    version1: result[1].text,
                    version2: result[0].text,
                    defaultValueSelect1: result[1].value,
                    defaultValueSelect2: result[0].value
                });
            } else if(result.length == 1) {
                self.getComparisonResult(result[0].value, result[0].value);
                self.setState({
                    version1: result[0].text,
                    version2: result[0].text,
                    defaultValueSelect1: result[0].value,
                    defaultValueSelect2: result[0].value
                });
                alert("只存在一个表版本信息");
            } else {
                alert("没有表版本信息");
            }
            self.setState({
                versionOptions: result
            });
        });
    },

    rowEqual: function (row1, row2) {
        return row1.columnName == row2.columnName;
    },

    getComparisonResult: function (ver1, ver2) {

        utils.showLoading();

        var param = {
            versionId1: ver1,
            versionId2: ver2
        };

        const self = this;

        $.get(utils.builPath("tables/getVersionDetail"), param, function (data) {
            utils.hideLoading();
            if (data.status != 200) {
                alert("查询表版本信息失败");
                return;
            }
            data = data.data;
            var v1data = data.v1data;
            var v2data = data.v2data;
            // v1data和v2data从0开始
            // dp和path从1开始
            var i, j;
            var dp = new Array(v1data.length + 1);
            for (i = 0; i <= v1data.length; i++) {
                dp[i] = new Array(v2data.length + 1);
                for (j = 0; j <= v2data.length; j++) {
                    dp[i][j] = 0;
                }
            }
            var path = new Array(v1data.length + 1);
            for (i = 0; i <= v1data.length; i++) {
                path[i] = new Array(v2data.length + 1);
                for (j = 0; j <= v2data.length; j++) {
                    path[i][j] = 0;
                }
            }

            const V1_LENGTH_SUB_1 = 1;
            const V2_LENGTH_SUB_1 = 2;
            const V1_AND_V2_LENGTH_SUB_1 = 3;

            for (i = 1; i <= v1data.length; i++) {
                path[i][0] = V1_LENGTH_SUB_1;
            }
            for (j = 1; j <= v2data.length; j++) {
                path[0][j] = V2_LENGTH_SUB_1;
            }

            for (i = 1; i <= v1data.length; i++) {
                for (j = 1; j <= v2data.length; j++) {
                    if (self.rowEqual(v1data[i - 1], v2data[j - 1])) {
                        path[i][j] = V1_AND_V2_LENGTH_SUB_1;
                        dp[i][j] = dp[i - 1][j - 1] + 1;
                    } else {
                        if (dp[i - 1][j] > dp[i][j - 1]) {
                            path[i][j] = V1_LENGTH_SUB_1;
                            dp[i][j] = dp[i - 1][j];
                        } else {
                            path[i][j] = V2_LENGTH_SUB_1;
                            dp[i][j] = dp[i][j - 1];
                        }
                    }
                }
            }

            var content = [];
            i = v1data.length;
            j = v2data.length;
            while (path[i][j] > 0) {
                if (path[i][j] == V1_LENGTH_SUB_1) {
                    content.push({
                        v1: v1data[i - 1],
                        v2: null
                    });
                    i--;
                } else if (path[i][j] == V2_LENGTH_SUB_1) {
                    content.push({
                        v1: null,
                        v2: v2data[j - 1]
                    });
                    j--;
                } else {
                    content.push({
                        v1: v1data[i - 1],
                        v2: v2data[j - 1]
                    });
                    i--;
                    j--;
                }
            }
            content.reverse();
            self.setState({
                content: content
            });
        });
    },

    getVersionText(version) {
        var versionOptions=this.state.versionOptions;
        for(var i=0;i<versionOptions.length;i++) {
            if(versionOptions[i].value==version) {
                return versionOptions[i].text;
            }
        }
    },

    createVersionComparison: function () {
        var ver1 = this.refs.ver1.getValue();
        var ver2 = this.refs.ver2.getValue();
        if (ver1 == -1 || ver2 == -1) {
            alert("请选择需要对比的两个版本");
            return;
        }
        this.getComparisonResult(ver1, ver2);
        var version1 = this.getVersionText(ver1);
        var version2 = this.getVersionText(ver2);
        this.setState({
            version1: version1,
            version2: version2
        });
    },

    comeBack:function(){
        this.props.history.pushState({passParam: "come back"}, "/data-table");
    },

    render: function () {
        var dsName = this.props.location.state.passParam.dsName;
        var schemaName = this.props.location.state.passParam.schemaName;
        var tableName = this.props.location.state.passParam.tableName;
        var title = "Table versions of " + dsName + "." + schemaName + "." + tableName;
        return (
            <TF>
                <TF.Header title={title}>
                    <Select
                        className="ui-select ui-select-long-width"
                        ref="ver1"
                        defaultValue={this.state.defaultValueSelect1}
                        defaultOpt={{value:-1, text:"version 1"}}
                        options={this.state.versionOptions}/>
                    <Select
                        className="ui-select ui-select-long-width"
                        ref="ver2"
                        defaultValue={this.state.defaultValueSelect2}
                        defaultOpt={{value:-1, text:"version 2"}}
                        options={this.state.versionOptions}/>
                    <B.Button
                        bsSize="sm"
                        bsStyle="warning"
                        onClick={this.createVersionComparison}>
                        <span className="glyphicon glyphicon-search">
                        </span> Compare
                    </B.Button>
                    <B.Button
                        bsSize="sm"
                        onClick={this.comeBack}>
                        Back
                    </B.Button>
                </TF.Header>
                <TF.Body>
                    <ComparisonTable content={this.state.content} version1={this.state.version1} version2={this.state.version2} />
                </TF.Body>
            </TF>
        );
    }
});

module.exports = TableVersionDifference;
