/**
 * Created by haowei6 on 2017/10/11.
 */

var React = require('react');
var ReactDOM = require('react-dom');
var B = require('react-bootstrap');
var $ = require('jquery');
var utils = require('../common/utils');
var antd = require('antd');
const RuleConst = require('./rule-const');
const defaultEqualValue = function (value) {
    if (value == '==' || value == '!=') return value;
    return '==';
};

const defaultRuleTypeValue = function (value) {
    if (value == 'string' || value == 'regex') return value;
    return 'string';
};

const defaultTrimOperate = function (value) {
    if (value == 'both' || value == 'left' || value == 'right' || value == 'all') return value;
    return 'both';
};

const defaultPrefixOrAppendOperate = function (value) {
    if (value == 'before' || value == 'after') return value;
    return 'before';
};

const defaultSubStringType = function (value) {
    if (value == 'string' || value == 'index') return value;
    return 'string';
};

const defaultSaveAsType = function (value) {
    if (value == 'STRING' || value == 'LONG' || value == 'DOUBLE' || value == 'DATETIME') return value;
    return 'STRING';
};

var EditRuleGrammarLine = React.createClass({

    handleRuleOperateSelectChange: function (value) {
        this.props.ruleGrammar[RuleConst.rule.ruleOperate] = value;
    },
    handleRuleTypeSelectChange: function (value) {
        this.props.ruleGrammar[RuleConst.rule.ruleType] = value;
    },
    handleSubStartTypeSelectChange: function (value) {
        this.props.ruleGrammar[RuleConst.rule.subStartType] = value;
    },
    handleSubEndTypeSelectChange: function (value) {
        this.props.ruleGrammar[RuleConst.rule.subEndType] = value;
    },
    handleTypeSelectChange: function (value) {
        this.props.ruleGrammar[RuleConst.rule.type] = value;
    },


    handleSubStartChange: function (event) {
        this.props.ruleGrammar[RuleConst.rule.subStart] = event.target.value;
    },
    handleSubEndChange: function (event) {
        this.props.ruleGrammar[RuleConst.rule.subEnd] = event.target.value;
    },
    handleRuleOperateChange: function (event) {
        this.props.ruleGrammar[RuleConst.rule.ruleOperate] = event.target.value;
    },
    handleRuleParamterChange: function (event) {
        this.props.ruleGrammar[RuleConst.rule.ruleParamter] = event.target.value;
    },
    handleRuleScopeChange: function (event) {
        this.props.ruleGrammar[RuleConst.rule.ruleScope] = event.target.value;
    },
    handleNameChange: function (event) {
        this.props.ruleGrammar[RuleConst.rule.name] = event.target.value;
    },
    handleFilterKeyChange: function (event) {
        this.props.ruleGrammar[RuleConst.rule.filterKey] = event.target.value;
    },


    handleAdd: function () {
        this.props.onAdd(this.props.no);
    },
    handleDelete: function () {
        this.props.onDelete(this.props.no);
    },
    handleMoveUp: function () {
        this.props.onMoveUp(this.props.no);
    },
    handleMoveDown: function () {
        this.props.onMoveDown(this.props.no);
    },

    render: function () {

        var ruleType = this.props.ruleType;
        var ruleGrammar = this.props.ruleGrammar;
        var no = this.props.no;
        var line = [];
        if (ruleType == RuleConst.type.flattenUms) {
            line.push(<antd.Input key={ruleGrammar.key+"Namespace"} addonBefore="Namespace"
                                  defaultValue={ruleGrammar[RuleConst.rule.ruleParamter]}
                                  onChange={this.handleRuleParamterChange}/>);
        } else if (ruleType == RuleConst.type.toIndex) {
            line.push(<antd.Input key={ruleGrammar.key+"Key"} addonBefore="Key"
                                  defaultValue={ruleGrammar[RuleConst.rule.ruleParamter]}
                                  onChange={this.handleRuleParamterChange}/>);
        } else if (ruleType == RuleConst.type.keyFilter) {
            line.push(<antd.Input key={ruleGrammar.key+"Key"} addonBefore="Key"
                                  defaultValue={ruleGrammar[RuleConst.rule.filterKey]}
                                  onChange={this.handleFilterKeyChange}/>);
        }


        if (ruleType != RuleConst.type.flattenUms && ruleType != RuleConst.type.keyFilter) {
            line.push(<antd.Input key={ruleGrammar.key+"Field"} addonBefore="Field"
                                  disabled={ruleType == RuleConst.type.toIndex}
                                  defaultValue={ruleGrammar[RuleConst.rule.ruleScope]}
                                  onChange={this.handleRuleScopeChange}/>);
        }

        if (ruleType == RuleConst.type.keyFilter) {
            line.push(
                <span className="ant-input-group-wrapper">
                    <span className="ant-input-wrapper ant-input-group">
                        <span className="ant-input-group-addon">Operate</span>
                        <antd.Select style={{ width: 100 }} key={ruleGrammar.key+"Operate"}
                                     defaultValue={ruleGrammar[RuleConst.rule.ruleOperate]=defaultEqualValue(ruleGrammar[RuleConst.rule.ruleOperate])}
                                     onChange={this.handleRuleOperateSelectChange}>
                            <antd.Select.Option value="==">include</antd.Select.Option>
                            <antd.Select.Option value="!=">exclude</antd.Select.Option>
                        </antd.Select>
                    </span>
                </span>
            );
            line.push(
                <span className="ant-input-group-wrapper">
                    <span className="ant-input-wrapper ant-input-group">
                        <span className="ant-input-group-addon">parameterType</span>
                        <antd.Select style={{ width: 100 }} key={ruleGrammar.key+"parameterType"}
                                     defaultValue={ruleGrammar[RuleConst.rule.ruleType]=defaultRuleTypeValue(ruleGrammar[RuleConst.rule.ruleType])}
                                     onChange={this.handleRuleTypeSelectChange}>
                            <antd.Select.Option value="string">string</antd.Select.Option>
                            <antd.Select.Option value="regex">regex</antd.Select.Option>
                        </antd.Select>
                    </span>
                </span>
            );
            line.push(<antd.Input key={ruleGrammar.key+"Parameter"} addonBefore="Parameter"
                                  defaultValue={ruleGrammar[RuleConst.rule.ruleParamter]}
                                  onChange={this.handleRuleParamterChange}/>);
        }
        else if (ruleType == RuleConst.type.filter) {
            line.push(
                <span className="ant-input-group-wrapper">
                    <span className="ant-input-wrapper ant-input-group">
                        <span className="ant-input-group-addon">Operate</span>
                        <antd.Select style={{ width: 100 }} key={ruleGrammar.key+"Operate"}
                                     defaultValue={ruleGrammar[RuleConst.rule.ruleOperate]=defaultEqualValue(ruleGrammar[RuleConst.rule.ruleOperate])}
                                     onChange={this.handleRuleOperateSelectChange}>
                            <antd.Select.Option value="==">include</antd.Select.Option>
                            <antd.Select.Option value="!=">exclude</antd.Select.Option>
                        </antd.Select>
                    </span>
                </span>
            );
            line.push(
                <span className="ant-input-group-wrapper">
                    <span className="ant-input-wrapper ant-input-group">
                        <span className="ant-input-group-addon">parameterType</span>
                        <antd.Select style={{ width: 100 }} key={ruleGrammar.key+"parameterType"}
                                     defaultValue={ruleGrammar[RuleConst.rule.ruleType]=defaultRuleTypeValue(ruleGrammar[RuleConst.rule.ruleType])}
                                     onChange={this.handleRuleTypeSelectChange}>
                            <antd.Select.Option value="string">string</antd.Select.Option>
                            <antd.Select.Option value="regex">regex</antd.Select.Option>
                        </antd.Select>
                    </span>
                </span>
            );
            line.push(<antd.Input key={ruleGrammar.key+"Parameter"} addonBefore="Parameter"
                                  defaultValue={ruleGrammar[RuleConst.rule.ruleParamter]}
                                  onChange={this.handleRuleParamterChange}/>);
        } else if (ruleType == RuleConst.type.trim) {
            line.push(
                <span className="ant-input-group-wrapper">
                    <span className="ant-input-wrapper ant-input-group">
                        <span className="ant-input-group-addon">Operate</span>
                        <antd.Select style={{ width: 100 }} key={ruleGrammar.key+"Operate"}
                                     defaultValue={ruleGrammar[RuleConst.rule.ruleOperate]=defaultTrimOperate(ruleGrammar[RuleConst.rule.ruleOperate])}
                                     onChange={this.handleRuleOperateSelectChange}>
                            <antd.Select.Option value="both">both</antd.Select.Option>
                            <antd.Select.Option value="left">left</antd.Select.Option>
                            <antd.Select.Option value="right">right</antd.Select.Option>
                            <antd.Select.Option value="all">all</antd.Select.Option>
                        </antd.Select>
                    </span>
                </span>
            );
            line.push(<antd.Input key={ruleGrammar.key+"Parameter"} addonBefore="Parameter"
                                  defaultValue={ruleGrammar[RuleConst.rule.ruleParamter]}
                                  onChange={this.handleRuleParamterChange}/>);
        } else if (ruleType == RuleConst.type.replace) {
            line.push(
                <span className="ant-input-group-wrapper">
                    <span className="ant-input-wrapper ant-input-group">
                        <span className="ant-input-group-addon">ruleType</span>
                        <antd.Select style={{ width: 100 }} key={ruleGrammar.key+"ruleType"}
                                     defaultValue={ruleGrammar[RuleConst.rule.ruleType]=defaultRuleTypeValue(ruleGrammar[RuleConst.rule.ruleType])}
                                     onChange={this.handleRuleTypeSelectChange}>
                            <antd.Select.Option value="string">string</antd.Select.Option>
                            <antd.Select.Option value="regex">regex</antd.Select.Option>
                        </antd.Select>
                    </span>
                </span>
            );
            line.push(<antd.Input key={ruleGrammar.key+"before"} addonBefore="before"
                                  defaultValue={ruleGrammar[RuleConst.rule.ruleParamter]}
                                  onChange={this.handleRuleParamterChange}/>);
            line.push(<antd.Input key={ruleGrammar.key+"after"} addonBefore="after"
                                  defaultValue={ruleGrammar[RuleConst.rule.ruleOperate]}
                                  onChange={this.handleRuleOperateChange}/>);
        } else if (ruleType == RuleConst.type.split) {
            line.push(
                <span className="ant-input-group-wrapper">
                    <span className="ant-input-wrapper ant-input-group">
                        <span className="ant-input-group-addon">parameterType</span>
                        <antd.Select style={{ width: 100 }} key={ruleGrammar.key+"parameterType"}
                                     defaultValue={ruleGrammar[RuleConst.rule.ruleType]=defaultRuleTypeValue(ruleGrammar[RuleConst.rule.ruleType])}
                                     onChange={this.handleRuleTypeSelectChange}>
                            <antd.Select.Option value="string">string</antd.Select.Option>
                            <antd.Select.Option value="regex">regex</antd.Select.Option>
                        </antd.Select>
                    </span>
                </span>
            );
            line.push(<antd.Input key={ruleGrammar.key+"token"} addonBefore="token"
                                  defaultValue={ruleGrammar[RuleConst.rule.ruleParamter]}
                                  onChange={this.handleRuleParamterChange}/>);
        }
        else if (ruleType == RuleConst.type.subString) {
            line.push(
                <span className="ant-input-group-wrapper">
                    <span className="ant-input-wrapper ant-input-group">
                        <span className="ant-input-group-addon">startType</span>
                        <antd.Select style={{ width: 100 }} key={ruleGrammar.key+"startType"}
                                     defaultValue={ruleGrammar[RuleConst.rule.subStartType]=defaultSubStringType(ruleGrammar[RuleConst.rule.subStartType])}
                                     onChange={this.handleSubStartTypeSelectChange}>
                            <antd.Select.Option value="string">string</antd.Select.Option>
                            <antd.Select.Option value="index">index</antd.Select.Option>
                        </antd.Select>
                    </span>
                </span>
            );
            line.push(<antd.Input key={ruleGrammar.key+"start"} addonBefore="start"
                                  defaultValue={ruleGrammar[RuleConst.rule.subStart]}
                                  onChange={this.handleSubStartChange}/>);
            line.push(
                <span className="ant-input-group-wrapper">
                    <span className="ant-input-wrapper ant-input-group">
                        <span className="ant-input-group-addon">endType</span>
                        <antd.Select style={{ width: 100 }} key={ruleGrammar.key+"endType"}
                                     defaultValue={ruleGrammar[RuleConst.rule.subEndType]=defaultSubStringType(ruleGrammar[RuleConst.rule.subEndType])}
                                     onChange={this.handleSubEndTypeSelectChange}>
                            <antd.Select.Option value="string">string</antd.Select.Option>
                            <antd.Select.Option value="index">index</antd.Select.Option>
                        </antd.Select>
                    </span>
                </span>
            );
            line.push(<antd.Input key={ruleGrammar.key+"end"} addonBefore="end"
                                  defaultValue={ruleGrammar[RuleConst.rule.subEnd]}
                                  onChange={this.handleSubEndChange}/>);
        }
        else if (ruleType == RuleConst.type.concat) {
            line.push(<antd.Input key={ruleGrammar.key+"parameter"} addonBefore="parameter"
                                  defaultValue={ruleGrammar[RuleConst.rule.ruleParamter]}
                                  onChange={this.handleRuleParamterChange}/>);
        }
        else if (ruleType == RuleConst.type.select) {

        }
        else if (ruleType == RuleConst.type.saveAs) {
            line.push(<antd.Input key={ruleGrammar.key+"Name"} addonBefore="Name"
                                  defaultValue={ruleGrammar[RuleConst.rule.name]}
                                  onChange={this.handleNameChange}/>);
            line.push(
                <span className="ant-input-group-wrapper">
                    <span className="ant-input-wrapper ant-input-group">
                        <span className="ant-input-group-addon">Type</span>
                        <antd.Select style={{ width: 100 }} key={ruleGrammar.key+"Type"}
                                     defaultValue={ruleGrammar[RuleConst.rule.type]=defaultSaveAsType(ruleGrammar[RuleConst.rule.type])}
                                     onChange={this.handleTypeSelectChange}>
                            <antd.Select.Option value="STRING">STRING</antd.Select.Option>
                            <antd.Select.Option value="LONG">LONG</antd.Select.Option>
                            <antd.Select.Option value="DOUBLE">DOUBLE</antd.Select.Option>
                            <antd.Select.Option value="DATETIME">DATETIME</antd.Select.Option>
                        </antd.Select>
                    </span>
                </span>
            );
        }
        else if (ruleType == RuleConst.type.prefixOrAppend) {
            line.push(
                <span className="ant-input-group-wrapper">
                    <span className="ant-input-wrapper ant-input-group">
                        <span className="ant-input-group-addon">Operator</span>
                        <antd.Select style={{ width: 100 }} key={ruleGrammar.key+"Operator"}
                                     defaultValue={ruleGrammar[RuleConst.rule.ruleOperate]=defaultPrefixOrAppendOperate(ruleGrammar[RuleConst.rule.ruleOperate])}
                                     onChange={this.handleRuleOperateSelectChange}>
                            <antd.Select.Option value="before">before</antd.Select.Option>
                            <antd.Select.Option value="after">after</antd.Select.Option>
                        </antd.Select>
                    </span>
                </span>
            );
            line.push(<antd.Input key={ruleGrammar.key+"parameter"} addonBefore="parameter"
                                  defaultValue={ruleGrammar[RuleConst.rule.ruleParamter]}
                                  onChange={this.handleRuleParamterChange}/>);
        }
        else if (ruleType == RuleConst.type.regexExtract) {
            line.push(<antd.Input key={ruleGrammar.key+"parameter"} addonBefore="parameter"
                                  defaultValue={ruleGrammar[RuleConst.rule.ruleParamter]}
                                  onChange={this.handleRuleParamterChange}/>);
        }

        line.push(<span className="ant-divider"/>);
        line.push(<antd.Button title="Add" icon="plus"
                               onClick={this.handleAdd}></antd.Button>);
        line.push(<span className="ant-divider"/>);
        line.push(<antd.Button title="Delete" icon="minus"
                               onClick={this.handleDelete}></antd.Button>);
        if (ruleType == RuleConst.type.toIndex) {
            line.push(<span className="ant-divider"/>);
            line.push(<antd.Button title="Move up" icon="caret-up"
                                   onClick={this.handleMoveUp}></antd.Button>);
            line.push(<span className="ant-divider"/>);
            line.push(<antd.Button title="Move down" icon="caret-down"
                                   onClick={this.handleMoveDown}></antd.Button>);
        }

        return (
            <div>
                {line}
            </div>
        );
    }
});

module.exports = EditRuleGrammarLine;
