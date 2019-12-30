/**
 * Created by haowei6 on 2017/10/11.
 */

import React, {PropTypes, Component} from 'react'
import {Select, Button, Input} from 'antd'
const Option = Select.Option
const defaultEqualValue = function (value) {
    if (value === '==' || value === '!=') return value;
    return '==';
};

const defaultRuleTypeValue = function (value) {
    if (value === 'string' || value === 'regex') return value;
    return 'string';
};

const defaultTrimOperate = function (value) {
    if (value === 'both' || value === 'left' || value === 'right' || value === 'all') return value;
    return 'both';
};

const defaultPrefixOrAppendOperate = function (value) {
    if (value === 'before' || value === 'after') return value;
    return 'before';
};

const defaultSubStringType = function (value) {
    if (value === 'string' || value === 'index') return value;
    return 'string';
};

const defaultSaveAsType = function (value) {
    if (value === 'STRING' || value === 'LONG' || value === 'DOUBLE' || value === 'DATETIME') return value;
    return 'STRING';
};

export default class EditRuleGrammarLine extends Component {

    handleRuleOperateSelectChange = (value) => {
        this.props.ruleGrammar.ruleOperate = value;
    }
    handleRuleTypeSelectChange = (value) => {
        this.props.ruleGrammar.ruleType = value;
    }
    handleSubStartTypeSelectChange = (value) => {
        this.props.ruleGrammar.subStartType = value;
    }
    handleSubEndTypeSelectChange = (value)  => {
        this.props.ruleGrammar.subEndType = value;
    }
    handleTypeSelectChange = (value) =>  {
        this.props.ruleGrammar.type = value;
    }


    handleSubStartChange = (event) =>  {
        this.props.ruleGrammar.subStart = event.target.value;
    }
    handleSubEndChange = (event) =>  {
        this.props.ruleGrammar.subEnd = event.target.value;
    }
    handleRuleOperateChange = (event) =>  {
        this.props.ruleGrammar.ruleOperate = event.target.value;
    }
    handleRuleParamterChange = (event) =>  {
        this.props.ruleGrammar.ruleParamter = event.target.value;
    }
    handleRuleScopeChange = (event) =>  {
        this.props.ruleGrammar.ruleScope = event.target.value;
    }
    handleNameChange = (event)  => {
        this.props.ruleGrammar.name = event.target.value;
    }
    handleFilterKeyChange = (event)  => {
        this.props.ruleGrammar.filterKey = event.target.value;
    }


    handleAdd = () =>  {
        this.props.onAdd(this.props.no);
    }
    handleDelete = () =>  {
        this.props.onDelete(this.props.no);
    }
    handleMoveUp = () =>  {
        this.props.onMoveUp(this.props.no);
    }
    handleMoveDown = () =>  {
        this.props.onMoveDown(this.props.no);
    }

    render = () => {

        let ruleType = this.props.ruleType;
        let ruleGrammar = this.props.ruleGrammar;
        let line = [];
        if (ruleType === 'flattenUms') {
            line.push(<Input key={ruleGrammar.key+"Namespace"} addonBefore="Namespace"
                                  defaultValue={ruleGrammar.ruleParamter}
                                  onChange={this.handleRuleParamterChange}/>);
        } else if (ruleType === 'toIndex') {
            line.push(<Input key={ruleGrammar.key+"toIndexInput1"} addonBefore="Key"
                                  defaultValue={ruleGrammar.ruleParamter}
                                  onChange={this.handleRuleParamterChange}/>);
        } else if (ruleType === 'keyFilter') {
            line.push(<Input key={ruleGrammar.key+"keyFilterInput1"} addonBefore="Key"
                             style={{width: 100}}
                                  defaultValue={ruleGrammar.filterKey}
                                  onChange={this.handleFilterKeyChange}/>);
        }


        if (ruleType !== 'flattenUms' && ruleType !== 'keyFilter') {
            line.push(<Input key={ruleGrammar.key+"Field"} addonBefore="Field"
                             style={ruleType === 'select' || ruleType === 'concat' ? {width: 300} : {width: 100}}
                                  disabled={ruleType === 'toIndex'}
                                  defaultValue={ruleGrammar.ruleScope}
                                  onChange={this.handleRuleScopeChange}/>);
        }

        if (ruleType === 'keyFilter') {
            line.push(
                <span key={ruleGrammar.key+"keyFilterSpan1"} className="ant-input-group-wrapper">
                    <span className="ant-input-wrapper ant-input-group">
                        <span className="ant-input-group-addon">Operate</span>
                        <Select style={{ width: 80 }} key={ruleGrammar.key+"Operate"}
                                     defaultValue={ruleGrammar.ruleOperate=defaultEqualValue(ruleGrammar.ruleOperate)}
                                     onChange={this.handleRuleOperateSelectChange}>
                            <Select.Option value="==">include</Select.Option>
                            <Select.Option value="!=">exclude</Select.Option>
                        </Select>
                    </span>
                </span>
            );
            line.push(
                <span key={ruleGrammar.key+"keyFilterSpan2"} className="ant-input-group-wrapper">
                    <span className="ant-input-wrapper ant-input-group">
                        <span className="ant-input-group-addon">parameterType</span>
                        <Select style={{ width: 80 }} key={ruleGrammar.key+"parameterType"}
                                     defaultValue={ruleGrammar.ruleType=defaultRuleTypeValue(ruleGrammar.ruleType)}
                                     onChange={this.handleRuleTypeSelectChange}>
                            <Select.Option value="string">string</Select.Option>
                            <Select.Option value="regex">regex</Select.Option>
                        </Select>
                    </span>
                </span>
            );
            line.push(<Input key={ruleGrammar.key+"keyFilterParameter"} addonBefore="Parameter"
                                  defaultValue={ruleGrammar.ruleParamter}
                                  onChange={this.handleRuleParamterChange}/>);
        }
        else if (ruleType === 'filter') {
            line.push(
                <span key={ruleGrammar.key+"filterSpan1"} className="ant-input-group-wrapper">
                    <span className="ant-input-wrapper ant-input-group">
                        <span className="ant-input-group-addon">Operate</span>
                        <Select style={{ width: 80 }} key={ruleGrammar.key+"Operate"}
                                     defaultValue={ruleGrammar.ruleOperate=defaultEqualValue(ruleGrammar.ruleOperate)}
                                     onChange={this.handleRuleOperateSelectChange}>
                            <Select.Option value="==">include</Select.Option>
                            <Select.Option value="!=">exclude</Select.Option>
                        </Select>
                    </span>
                </span>
            );
            line.push(
                <span key={ruleGrammar.key+"filterSpan2"} className="ant-input-group-wrapper">
                    <span className="ant-input-wrapper ant-input-group">
                        <span className="ant-input-group-addon">parameterType</span>
                        <Select style={{ width: 80 }} key={ruleGrammar.key+"parameterType"}
                                     defaultValue={ruleGrammar.ruleType=defaultRuleTypeValue(ruleGrammar.ruleType)}
                                     onChange={this.handleRuleTypeSelectChange}>
                            <Select.Option value="string">string</Select.Option>
                            <Select.Option value="regex">regex</Select.Option>
                        </Select>
                    </span>
                </span>
            );
            line.push(<Input key={ruleGrammar.key+"filterParameter"} addonBefore="Parameter"
                                  defaultValue={ruleGrammar.ruleParamter}
                                  onChange={this.handleRuleParamterChange}/>);
        } else if (ruleType === 'trim') {
            line.push(
                <span key={ruleGrammar.key+"trimSpan1"} className="ant-input-group-wrapper">
                    <span className="ant-input-wrapper ant-input-group">
                        <span className="ant-input-group-addon">Operate</span>
                        <Select style={{ width: 100 }} key={ruleGrammar.key+"Operate"}
                                     defaultValue={ruleGrammar.ruleOperate=defaultTrimOperate(ruleGrammar.ruleOperate)}
                                     onChange={this.handleRuleOperateSelectChange}>
                            <Select.Option value="both">both</Select.Option>
                            <Select.Option value="left">left</Select.Option>
                            <Select.Option value="right">right</Select.Option>
                            <Select.Option value="all">all</Select.Option>
                        </Select>
                    </span>
                </span>
            );
            line.push(<Input key={ruleGrammar.key+"trimParameter"} addonBefore="Parameter"
                                  defaultValue={ruleGrammar.ruleParamter}
                                  onChange={this.handleRuleParamterChange}/>);
        } else if (ruleType === 'replace') {
            line.push(
                <span key={ruleGrammar.key+"replaceSpan1"} className="ant-input-group-wrapper">
                    <span className="ant-input-wrapper ant-input-group">
                        <span className="ant-input-group-addon">ruleType</span>
                        <Select style={{ width: 100 }} key={ruleGrammar.key+"ruleType"}
                                     defaultValue={ruleGrammar.ruleType=defaultRuleTypeValue(ruleGrammar.ruleType)}
                                     onChange={this.handleRuleTypeSelectChange}>
                            <Select.Option value="string">string</Select.Option>
                            <Select.Option value="regex">regex</Select.Option>
                        </Select>
                    </span>
                </span>
            );
            line.push(<Input key={ruleGrammar.key+"before"} addonBefore="before"
                                  defaultValue={ruleGrammar.ruleParamter}
                                  onChange={this.handleRuleParamterChange}/>);
            line.push(<Input key={ruleGrammar.key+"after"} addonBefore="after"
                                  defaultValue={ruleGrammar.ruleOperate}
                                  onChange={this.handleRuleOperateChange}/>);
        } else if (ruleType === 'split') {
            line.push(
                <span key={ruleGrammar.key+"splitSpan1"} className="ant-input-group-wrapper">
                    <span className="ant-input-wrapper ant-input-group">
                        <span className="ant-input-group-addon">parameterType</span>
                        <Select style={{ width: 100 }} key={ruleGrammar.key+"parameterType"}
                                     defaultValue={ruleGrammar.ruleType=defaultRuleTypeValue(ruleGrammar.ruleType)}
                                     onChange={this.handleRuleTypeSelectChange}>
                            <Select.Option value="string">string</Select.Option>
                            <Select.Option value="regex">regex</Select.Option>
                        </Select>
                    </span>
                </span>
            );
            line.push(<Input key={ruleGrammar.key+"token"} addonBefore="token"
                                  defaultValue={ruleGrammar.ruleParamter}
                                  onChange={this.handleRuleParamterChange}/>);
        }
        else if (ruleType === 'subString') {
            line.push(
                <span key={ruleGrammar.key+"subStringSpan1"} className="ant-input-group-wrapper">
                    <span className="ant-input-wrapper ant-input-group">
                        <span className="ant-input-group-addon">startType</span>
                        <Select style={{ width: 80 }} key={ruleGrammar.key+"startType"}
                                     defaultValue={ruleGrammar.subStartType=defaultSubStringType(ruleGrammar.subStartType)}
                                     onChange={this.handleSubStartTypeSelectChange}>
                            <Select.Option value="string">string</Select.Option>
                            <Select.Option value="index">index</Select.Option>
                        </Select>
                    </span>
                </span>
            );
            line.push(<Input key={ruleGrammar.key+"start"} addonBefore="start"
                                  defaultValue={ruleGrammar.subStart}
                                  onChange={this.handleSubStartChange}/>);
            line.push(
                <span key={ruleGrammar.key+"trimSpan2"} className="ant-input-group-wrapper">
                    <span className="ant-input-wrapper ant-input-group">
                        <span className="ant-input-group-addon">endType</span>
                        <Select style={{ width: 80 }} key={ruleGrammar.key+"endType"}
                                     defaultValue={ruleGrammar.subEndType=defaultSubStringType(ruleGrammar.subEndType)}
                                     onChange={this.handleSubEndTypeSelectChange}>
                            <Select.Option value="string">string</Select.Option>
                            <Select.Option value="index">index</Select.Option>
                        </Select>
                    </span>
                </span>
            );
            line.push(<Input key={ruleGrammar.key+"end"} addonBefore="end"
                                  defaultValue={ruleGrammar.subEnd}
                                  onChange={this.handleSubEndChange}/>);
        }
        else if (ruleType === 'concat') {
            line.push(<Input key={ruleGrammar.key+"parameter"} addonBefore="parameter"
                                  defaultValue={ruleGrammar.ruleParamter}
                                  onChange={this.handleRuleParamterChange}/>);
        }
        else if (ruleType === 'select') {

        }
        else if (ruleType === 'saveAs') {
            line.push(<Input key={ruleGrammar.key+"Name"} addonBefore="Name"
                                  defaultValue={ruleGrammar.name}
                                  onChange={this.handleNameChange}/>);
            line.push(
                <span key={ruleGrammar.key+"saveAsSpan1"} className="ant-input-group-wrapper">
                    <span className="ant-input-wrapper ant-input-group">
                        <span className="ant-input-group-addon">Type</span>
                        <Select style={{ width: 100 }} key={ruleGrammar.key+"Type"}
                                     defaultValue={ruleGrammar.type=defaultSaveAsType(ruleGrammar.type)}
                                     onChange={this.handleTypeSelectChange}>
                            <Select.Option value="STRING">STRING</Select.Option>
                            <Select.Option value="LONG">LONG</Select.Option>
                            <Select.Option value="DOUBLE">DOUBLE</Select.Option>
                            <Select.Option value="DATETIME">DATETIME</Select.Option>
                        </Select>
                    </span>
                </span>
            );
        }
        else if (ruleType === 'prefixOrAppend') {
            line.push(
                <span key={ruleGrammar.key+"prefixOrAppendSpan1"} className="ant-input-group-wrapper">
                    <span className="ant-input-wrapper ant-input-group">
                        <span className="ant-input-group-addon">Operator</span>
                        <Select style={{ width: 100 }} key={ruleGrammar.key+"Operator"}
                                     defaultValue={ruleGrammar.ruleOperate=defaultPrefixOrAppendOperate(ruleGrammar.ruleOperate)}
                                     onChange={this.handleRuleOperateSelectChange}>
                            <Select.Option value="before">before</Select.Option>
                            <Select.Option value="after">after</Select.Option>
                        </Select>
                    </span>
                </span>
            );
            line.push(<Input key={ruleGrammar.key+"parameter"} addonBefore="parameter"
                                  defaultValue={ruleGrammar.ruleParamter}
                                  onChange={this.handleRuleParamterChange}/>);
        }
        else if (ruleType === 'regexExtract') {
            line.push(<Input key={ruleGrammar.key+"parameter"} addonBefore="parameter"
                                  defaultValue={ruleGrammar.ruleParamter}
                                  onChange={this.handleRuleParamterChange}/>);
        }
        else if (ruleType === 'jsonPath') {
            line.push(<Input key={ruleGrammar.key+"parameter"} addonBefore="parameter"
                                  style={{width: 700}}
                                  defaultValue={ruleGrammar.ruleParamter}
                                  onChange={this.handleRuleParamterChange}/>);
        }
        else if (ruleType === 'flattenJsonArray') {
          line.push(<Input key={ruleGrammar.key+"parameter"} addonBefore="parameter"
                           style={{width: 700}}
                           defaultValue={ruleGrammar.ruleParamter}
                           onChange={this.handleRuleParamterChange}/>);
        }

        line.push(<span key={ruleGrammar.key+"tailSpan1"} className="ant-divider"/>);
        line.push(<Button key={ruleGrammar.key+"buttonAdd"} shape="circle" title="Add" icon="plus"
                               onClick={this.handleAdd}/>);
        line.push(<span key={ruleGrammar.key+"tailSpan2"} className="ant-divider"/>);
        line.push(<Button key={ruleGrammar.key+"buttonDelete"} shape="circle" title="Delete" icon="minus"
                               onClick={this.handleDelete}/>);
        if (ruleType === 'toIndex') {
            line.push(<span key={ruleGrammar.key+"tailSpan3"} className="ant-divider"/>);
            line.push(<Button key={ruleGrammar.key+"buttonMoveUp"} shape="circle" title="Move up" icon="arrow-up"
                                   onClick={this.handleMoveUp}/>);
            line.push(<span key={ruleGrammar.key+"tailSpan4"} className="ant-divider"/>);
            line.push(<Button key={ruleGrammar.key+"buttonMoveDown"} shape="circle" title="Move down" icon="arrow-down"
                                   onClick={this.handleMoveDown}/>);
        }

        return (
            <div>
                {line}
            </div>
        );
    }
}
