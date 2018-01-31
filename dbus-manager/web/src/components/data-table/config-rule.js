var React = require('react');
var ReactDOM = require('react-dom');
var antd = require('antd');
var $ = require('jquery');
var utils = require('../common/utils');
var B = require('react-bootstrap');
var CustomCompareTable = require('./custom-compare-table');
const RuleConst = require('./rule-const');


const STATUS = {
    ACTIVE: "active",
    INACTIVE: "inactive"
};

const MODAL_TYPE = {
    CHANGE_GROUP_NAME: "change_group_name",
    ADD_NEW_GROUP: "add_new_group",
    CLONE_GROUP: "clone_group"
};
var ConfigRule = React.createClass({

    getInitialState: function () {
        return {

            // 改名对话框
            visible: false,
            value: null,
            groupInfo: null,
            modalType: null,

            //Diff对话框
            diffVisible: false,
            title: [],
            subTitle: [],
            content: [],

            //checkbox选中列表
            checkboxList:[],

            groups: [],
            groupColumns: [{
                title: "ID",
                key: "id",
                width: 80,
                render: (text) => (
                    <div style={{width: 80}}>
                        <antd.Checkbox onChange={(event) => this.changeCheckbox(event, text)}>{text.id}</antd.Checkbox>
                    </div>
                )
            }, {
                title: "Name",
                key: RuleConst.group.groupName,
                render: (text) => (
                    <a title="Edit rules" onClick={() => this.editRuleGroup(text)}>{text[RuleConst.group.groupName]}</a>
                )
            }, {
                title: "Status",
                key: RuleConst.group.status,
                width: 60,
                render: (text) => (
                    <antd.Switch defaultChecked={text[RuleConst.group.status] == STATUS.ACTIVE}
                                 onChange={() => this.changeStatus(text)}/>
                )
            }, {
                title: "Operation",
                key: "operation",
                width: 140,
                render: (text) => (
                    <span>
                        <antd.Button title="Rename" icon="edit"
                                     onClick={() => this.onChangeName(text)}></antd.Button>
                        <span className="ant-divider"/>
                        <antd.Button title="Clone" icon="copy"
                                     onClick={() => this.onCloneGroup(text)}></antd.Button>
                        <span className="ant-divider"/>
                        <antd.Button title="Delete" icon="close-circle"
                                     onClick={() => this.onDeleteRuleGroup(text)}></antd.Button>
                    </span>
                )
            }, {
                title: "Schema",
                key: "schema",
                render: (text) => {
                    var list = text[RuleConst.type.saveAs];
                    if(utils.isEmpty(list)) return <div></div>;
                    list = JSON.parse(list);
                    list = list.map(function (item) {
                        return <li>{'Field '+item[RuleConst.rule.ruleScope]+': '+item[RuleConst.rule.name]+'('+item[RuleConst.rule.type]+')'}</li>
                    });
                    return (
                        <antd.Popover content={<ul>{list}</ul>} title="Schema">
                            {text[RuleConst.type.saveAs].substr(0,20) + '...'}
                        </antd.Popover>
                    )
                },
                width: 140
            }, {
                title: "Update Time",
                key: "updateTime",
                dataIndex: "updateTime",
                width: 160
            }]
        }
    },

    getGroupFromResultData: function (result) {
        var group = result.data.group;
        var saveAs = result.data[RuleConst.type.saveAs];
        for (var i = 0; i < group.length; i++) {
            group[i].key = i;
            saveAs.forEach(function (item) {
                if (item[RuleConst.rule.groupId] == group[i].id) {
                    group[i][RuleConst.type.saveAs] = item[RuleConst.rule.ruleGrammar];
                }
            });
        }
        return group;
    },

    componentWillMount: function () {
        var self = this;
        var param = {
            tableId: this.props.location.state.passParam.tableId
        };

        $.get(utils.builPath("tables/getAllRuleGroup"), param, function (result) {
            utils.hideLoading();
            if (result.status != 200) {
                alert("Load rule group failed!");
                return;
            }

            self.setState({groups: self.getGroupFromResultData(result)});
        });
    },

    changeCheckbox: function (event, text) {
        var checkboxList = this.state.checkboxList;
        var checked = event.target.checked;
        if (checked) {
            checkboxList.push(text.id);
        } else {
            for (let i = 0; i < checkboxList.length; i++) {
                if (checkboxList[i] == text.id) {
                    checkboxList.splice(i, 1);
                    break;
                }
            }
        }
        this.setState({
            checkboxList: checkboxList
        });
    },

    onChangeName: function (text) {
        this.setState({
            visible: true,
            value: text.groupName,
            groupInfo: text,
            modalType: MODAL_TYPE.CHANGE_GROUP_NAME
        });
    },

    onCloneGroup: function (text) {
        this.setState({
            visible: true,
            value: text.groupName + '-clone',
            groupInfo: text,
            modalType: MODAL_TYPE.CLONE_GROUP
        });
    },

    changeStatus: function (text) {
        var newStatus = STATUS.ACTIVE;
        if (text.status == STATUS.ACTIVE) {
            newStatus = STATUS.INACTIVE;
        }

        var param = {
            tableId: this.props.location.state.passParam.tableId,
            groupId: text.id,
            newStatus: newStatus,
            newName: text[RuleConst.group.groupName]
        };

        this.updateGroupInfo(param);
    },

    cloneGroup: function (param) {
        var self = this;
        utils.showLoading();
        $.get(utils.builPath("tables/cloneRuleGroup"), param, function (result) {
            utils.hideLoading();
            if (result.status != 200) {
                alert("clone rule group failed!");
                return;
            }

            self.setState({groups: self.getGroupFromResultData(result)});
        });
    },

    updateGroupInfo: function (param) {
        var self = this;
        utils.showLoading();
        $.get(utils.builPath("tables/updateRuleGroup"), param, function (result) {
            utils.hideLoading();
            if (result.status != 200) {
                alert("update rule group information failed!");
                return;
            }

            self.setState({groups: self.getGroupFromResultData(result)});
        });
    },

    editRuleGroup: function (text) {
        var passParam = {
            tableId: this.props.location.state.passParam.tableId,
            dsId: this.props.location.state.passParam.dsId,
            dsName: this.props.location.state.passParam.dsName,
            dsType: this.props.location.state.passParam.dsType,
            schemaName: this.props.location.state.passParam.schemaName,
            tableName: this.props.location.state.passParam.tableName,

            groupId: text.id,
            groupName: text.groupName,
            status: text.status
        };

        utils.showLoading();
        this.props.history.pushState({passParam: passParam}, "/data-table/edit-rule");
    },

    onDeleteRuleGroup: function (text) {
        if (!confirm("Are you sure to delete this group?")) {
            return;
        }

        var param = {
            tableId: this.props.location.state.passParam.tableId,
            groupId: text.id
        };

        var self = this;
        utils.showLoading();
        $.get(utils.builPath("tables/deleteRuleGroup"), param, function (result) {
            utils.hideLoading();
            if (result.status != 200) {
                alert("delete rule group failed!");
                return;
            }
            self.setState({groups: self.getGroupFromResultData(result)});
        });
    },

    onUpgradeVersion: function () {

        var self = this;

        var sendLogProcessorReloadMessage = function (message) {
            var param = {
                ds: self.props.location.state.passParam.dsId,
                ctrlTopic: self.props.location.state.passParam.dsName + '_ctrl',
                message: message
            };
            $.get(utils.builPath('ctlMessage/send'), param, function(result) {
                utils.hideLoading();
                if(result.status === 200) {
                    alert("Upgrade version success");
                } else {
                    alert(message.type + " 消息发送失败！");
                }
            });
        };

        var getLogProcessorReloadTemplate = function () {
            $.get(utils.builPath("ctlMessage"), null, function (result) {
                if (result.status != 200) {
                    utils.hideLoading();
                    alert("Reload Log Processor failed!");
                    return;
                }
                var data = result.data;
                data = data.filter(function (value) {
                    return value.type == "LOG_PROCESSOR_RELOAD_CONFIG";
                });
                if (utils.isEmpty(data)) {
                    utils.hideLoading();
                    alert("加载 Log-Processor Reload 模板信息失败");
                    return;
                }
                data = data[0];
                data = data.template;
                data.payload.dsName = self.props.location.state.passParam.dsName;
                data.payload.dsType = self.props.location.state.passParam.dsType;
                sendLogProcessorReloadMessage(data);
            });
        };

        var param = {
            tableId: this.props.location.state.passParam.tableId,
            dsId: this.props.location.state.passParam.dsId,
            dsName: this.props.location.state.passParam.dsName,
            schemaName: this.props.location.state.passParam.schemaName,
            tableName: this.props.location.state.passParam.tableName
        };
        utils.showLoading();
        $.get(utils.builPath("tables/upgradeVersion"), param, function (result) {
            if (result.status != 200) {
                utils.hideLoading();
                if (result.message) {
                    alert(result.message);
                } else {
                    alert("Upgrade rule version failed!");
                }
                return;
            }
            getLogProcessorReloadTemplate();
        });
    },

    onDiff: function () {
        var self = this;
        var param = {
            tableId: this.props.location.state.passParam.tableId
        };
        $.get(utils.builPath("tables/diffGroupRule"), param, function (result) {
            if (result.status != 200) {
                alert("Get rule info failed!");
                return;
            }

            var checkboxList = self.state.checkboxList;
            var data = result.data;

            var newData = [];
            checkboxList.forEach(function (id) {
                data.forEach(function (group) {
                    if (group[RuleConst.rule.groupId] == id) newData.push(group);
                });
            });
            data = newData;

            var title = data.map(function (group) {
                return group[RuleConst.group.groupName];
            });

            var subTitle = [RuleConst.rule.name, RuleConst.rule.type];

            var content = data.map(function (group) {
                var grammars = group[RuleConst.rule.ruleGrammar];
                if(grammars == null) return [];
                var ret = [];
                grammars = JSON.parse(grammars);
                grammars.forEach(function (grammar) {
                    ret[grammar[RuleConst.rule.ruleScope] - 0] = [grammar[RuleConst.rule.name], grammar[RuleConst.rule.type]];
                });
                return ret;
            });

            self.setState({
                diffVisible: true,
                title: title,
                subTitle: subTitle,
                content: content
            })

        });
    },

    onAddGroup: function () {
        this.setState({
            visible: true,
            value: null,
            groupInfo: null,
            modalType: MODAL_TYPE.ADD_NEW_GROUP
        });
    },

    addGroup: function (param) {
        var self = this;
        utils.showLoading();
        $.get(utils.builPath("tables/addGroup"), param, function (result) {
            utils.hideLoading();
            if (result.status != 200) {
                alert("add rule group failed!");
                return;
            }
            self.setState({groups: self.getGroupFromResultData(result)});
        });
    },

    onModalOk: function (e) {
        var newName = this.state.value;
        if (newName == null) {
            alert("Name can not be empty!");
            return;
        }
        newName = utils.trim(newName);
        if (newName == "") {
            alert("Name can not be empty!");
            return;
        }

        this.setState({
            visible: false
        });

        if (this.state.modalType == MODAL_TYPE.CHANGE_GROUP_NAME) {
            var param = {
                tableId: this.props.location.state.passParam.tableId,
                groupId: this.state.groupInfo.id,
                newStatus: this.state.groupInfo.status,
                newName: newName
            };
            this.updateGroupInfo(param);
        } else if (this.state.modalType == MODAL_TYPE.ADD_NEW_GROUP) {
            var param = {
                tableId: this.props.location.state.passParam.tableId,
                newName: newName,
                newStatus: STATUS.INACTIVE
            };
            this.addGroup(param);
        } else if (this.state.modalType == MODAL_TYPE.CLONE_GROUP) {
            var param = {
                tableId: this.props.location.state.passParam.tableId,
                groupId: this.state.groupInfo.id,
                newStatus: STATUS.INACTIVE,
                newName: newName
            };
            this.cloneGroup(param);
        }
    },
    onModalCancel: function (e) {
        this.setState({
            visible: false
        });
    },
    onModalInputChange: function (e) {
        this.setState({
            value: e.target.value
        });
    },
    /**
     * 传递过来的消息必须包括以下项
     var passParam = {
            tableId: obj.id,
            dsId: obj.dsID,
            dsName: obj.dsName,
            dsType: obj.dsType,
            schemaName: obj.schemaName,
            tableName: obj.tableName
        };
     */

    render: function () {
        const schemaName = this.props.location.state.passParam.schemaName;
        const tableName = this.props.location.state.passParam.tableName;
        return (
            <div className="container-fluid">
                <div className="row header">
                    <h4 className="col-xs-6">
                        {"Table: " + schemaName + "." + tableName}
                    </h4>
                    <h4 className="col-xs-6">
                        <div style={{float:"right"}}>
                            <antd.Button icon="sync" type="primary" onClick={this.onUpgradeVersion}>Upgrade version
                            </antd.Button>
                            <span className="ant-divider"/>
                            <antd.Button icon="exception" onClick={this.onDiff}>Diff</antd.Button>
                            <span className="ant-divider"/>
                            <antd.Button icon="plus-circle" onClick={this.onAddGroup}>Add group</antd.Button>
                        </div>
                    </h4>
                    <div style={{overflowX:"auto",overflowY:"auto",width:"100%"}} className="col-xs-12">
                        <antd.Table locale={{emptyText:"No group"}}
                                    pagination={false}
                                    bordered
                                    size="default"
                                    dataSource={this.state.groups}
                                    columns={this.state.groupColumns}>
                        </antd.Table>
                    </div>
                </div>

                <antd.Modal title="Please input new rule group name" visible={this.state.visible} onOk={this.onModalOk}
                            onCancel={this.onModalCancel}>
                    <antd.Input placeholder="Name can not be empty" value={this.state.value}
                                onChange={this.onModalInputChange} onPressEnter={this.onModalOk}/>
                </antd.Modal>

                <antd.Modal width={1280} visible={this.state.diffVisible}
                            onOk={() => this.setState({diffVisible: false})}
                            onCancel={() => this.setState({diffVisible: false})}>
                    <CustomCompareTable title={this.state.title} subTitle={this.state.subTitle} content={this.state.content}/>
                </antd.Modal>
            </div>
        );
    }
});

module.exports = ConfigRule;
