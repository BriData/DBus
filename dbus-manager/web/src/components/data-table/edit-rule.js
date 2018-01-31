var React = require('react');
var ReactDOM = require('react-dom');
var antd = require('antd');
var $ = require('jquery');
var utils = require('../common/utils');
var JSONEditor = require('jsoneditor');
const EditRuleGrammar = require('./edit-rule-grammar');
const RuleConst = require('./rule-const');
const ShouldUpdateAntdTable = require('./should-update-antd-table');

var RuleTypeTips = {};

RuleTypeTips[RuleConst.type.flattenUms] = (
    <div>
        <p>将UMS消息中的所有数据提取出来</p>
        <p>每个tuple作为单独一行数据</p>
    </div>
);
RuleTypeTips[RuleConst.type.toIndex] = (
    <div>
        <p>将JSON源数据按Key映射为下标</p>
        <p>下标从小到大排序,从0开始</p>
    </div>
);
RuleTypeTips[RuleConst.type.filter] = (
    <div>
        <p>Field为1，Operate为include，ParameterType为string，Parameter为aaa</p>
        <p>第1列包含字符串"aaa"</p>
        <p>eg.123aaa456</p>
        <br/>
        <p>Field为1，Operate为include，ParameterType为regex，Parameter为\d</p>
        <p>第1列包含正则表达式指定内容</p>
        <p>eg.123aaa456</p>
    </div>
);
RuleTypeTips[RuleConst.type.trim] = (
    <div>
        <p>Field为1，Operate为both，Parameter为abc</p>
        <p>trim第1列内容，abcXXXXXcab 变为 XXXXX</p>
    </div>
);
RuleTypeTips[RuleConst.type.replace] = (
    <div>
        <p>Field为1，RuleType为string，before为aaa，after为123</p>
        <p>把1列中的"aaa"替换成123</p>
        <p>eg. 456aaa789bbb 替换结果 456123789bbb</p>
        <br/>
        <p>Field为1，RuleType为string，before为\d&#123;3&#125;，after为aaa</p>
        <p>把1列中的\d&#123;3&#125;替换成aaa</p>
        <p>eg. 456aaa789bbb 替换结果aaaaaaaaabbb</p>
        <br/>
        <p>正则支持分组 例如：</p>
        <p>字符串： ABC02-04XYZ</p>
        <p>正则： (\d&#123;2&#125;)(.*)(\d&#123;2&#125;)</p>
        <p>替换为：$3 *** $1</p>
        <p>结果为   ABC04 *** 02XYZ</p>
    </div>
);
RuleTypeTips[RuleConst.type.split] = (
    <div>
        <p>Field为1，ParameterType为string，token为aaa</p>
        <p>第1列用"aaa"分隔</p>
        <p>eg.123aaa456，分隔结果 123、456</p>
        <br/>
        <p>Field为1，ParameterType为regex，token为\d&#123;3&#125;</p>
        <p>第1列用\d&#123;3&#125;正则分隔</p>
        <p>eg.bbb123aaa456ccc，分隔结果bbb、aaa、ccc</p>
        <br/>
        <p>如果对多个字符做split 可以用</p>
        <p>[.:] 或 (aaa)|(bbb)</p>
    </div>
);
RuleTypeTips[RuleConst.type.subString] = (
    <div>
        <p>Field为1</p>
        <br/>
        <p>startType为string，start为aaa，endType为string，end为bbb</p>
        <p>把第1列正向搜索aaa的第一个出现位置截取到，ccc逆向搜索第一个出现的位置</p>
        <p>eg. 123aaa456789ccc1111 </p>
        <p>截取结果: aaa456789ccc</p>
        <br/>
        <p>startType为index，start为1，endType为index，end为5</p>
        <p>把第1列从索引位置1截取到索引位置5</p>
        <p>eg. 0123456789 </p>
        <p>截取结果: 1234</p>
        <br/>
        <p>startType为index，start为1，endType为string，end为ccc</p>
        <p>把第1列从索引位置1截取到,ccc逆向出现的第一个位置处</p>
        <p>eg. 0123456ccc78ccc9 </p>
        <p>截取结果: 123456ccc78ccc</p>
    </div>
);
RuleTypeTips[RuleConst.type.concat] = (
    <div>
        <p>Field为1，3，5，-1，Parameter为#</p>
        <p>把1、3、5、最后一列用#链接</p>
        <p>eg.aa#bb#cc#dd</p>
    </div>
);
RuleTypeTips[RuleConst.type.select] = (
    <div>
        <p>Field为1，3，5，-1</p>
        <p>从所有列中提取出第1、3、最后一列</p>
    </div>
);
RuleTypeTips[RuleConst.type.saveAs] = (
    <div>
        <p>Field为1, name为"时间"，type为"DATETIME"</p>
        <p>指定第1列的名称为：时间，类型为：DATETIME</p>
    </div>
);
RuleTypeTips[RuleConst.type.prefixOrAppend] = (
    <div>
        <p>Field为1, Operator为"after"，parameter为"aaa"</p>
        <p>在第一列的前面拼接上常量字符串"aaa"，第一列原始数据为123456，结果为aaa123456</p>
    </div>
);
RuleTypeTips[RuleConst.type.regexExtract] = (
    <div>
        <p>'Field为1, parameter为(\d&#123;4&#125;年\d&#123;1,2&#125;月)已结清'</p>
        <p>从第1列中提取出****年**月</p>
        <p>eg. 2017年11月已结清</p>
        <p>提取结果：2017年11月</p>
    </div>
);

var editor = null;
var EditRule = React.createClass({

    getInitialState: function () {
        return {
            kafkaTopic: "topic",
            kafkaOffset: -1,
            kafkaCount: -1,

            resultSource: [],
            resultColumns: [],

            visible: false,
            umsContent: {},

            detailVisible: false,
            detailTitle: null,
            detailContent: null,

            rules: [],
            ruleColumns: [{
                title: "Order",
                key: RuleConst.rule.orderId,
                dataIndex: RuleConst.rule.orderId
            }, {
                title: "Type",
                key: RuleConst.rule.ruleTypeName,
                render: (text) => (
                    <span>
                        <span className="ant-input-group-wrapper">
                            <span className="ant-input-wrapper ant-input-group">
                                <antd.Select value={text[RuleConst.rule.ruleTypeName]} style={{ width: "120" }}
                                             onChange={(value) => this.onSelectChange(text, value)}>
                                <antd.Select.Option value={RuleConst.type.flattenUms} disabled={this.props.location.state.passParam.dsType != utils.dsType.logUms} >{RuleConst.type.flattenUms}</antd.Select.Option>
                                <antd.Select.Option value={RuleConst.type.keyFilter}>{RuleConst.type.keyFilter}</antd.Select.Option>
                                <antd.Select.Option value={RuleConst.type.toIndex}>{RuleConst.type.toIndex}</antd.Select.Option>
                                <antd.Select.Option value={RuleConst.type.filter}>{RuleConst.type.filter}</antd.Select.Option>
                                <antd.Select.Option value={RuleConst.type.trim}>{RuleConst.type.trim}</antd.Select.Option>
                                <antd.Select.Option value={RuleConst.type.replace}>{RuleConst.type.replace}</antd.Select.Option>
                                <antd.Select.Option value={RuleConst.type.split}>{RuleConst.type.split}</antd.Select.Option>
                                <antd.Select.Option value={RuleConst.type.subString}>{RuleConst.type.subString}</antd.Select.Option>
                                <antd.Select.Option value={RuleConst.type.concat}>{RuleConst.type.concat}</antd.Select.Option>
                                <antd.Select.Option value={RuleConst.type.select}>{RuleConst.type.select}</antd.Select.Option>
                                <antd.Select.Option value={RuleConst.type.saveAs}>{RuleConst.type.saveAs}</antd.Select.Option>
                                <antd.Select.Option value={RuleConst.type.prefixOrAppend}>{RuleConst.type.prefixOrAppend}</antd.Select.Option>
                                <antd.Select.Option value={RuleConst.type.regexExtract}>{RuleConst.type.regexExtract}</antd.Select.Option>
                                </antd.Select>
                            </span>
                      </span>
                      <span style={{marginLeft: "5px"}}>
                          <antd.Popover content={RuleTypeTips[text[RuleConst.rule.ruleTypeName]]} title={text[RuleConst.rule.ruleTypeName]}>
                              <antd.Icon type="question-circle-o"/>
                          </antd.Popover>
                      </span>
                   </span>
                )
            }, {
                title: "Args",
                key: RuleConst.rule.ruleGrammar,
                render: (text) => (
                    <EditRuleGrammar onRuleGrammarChange={this.onRuleGrammarChange} rule={text}/>
                )
            }, {
                title: "Opetation",
                key: "operation",
                width: 180,
                render: (text) => (
                    <span>
                        <antd.Button title="Move up" icon="caret-up" size="default"
                                     onClick={() => this.moveUp(text.key)}></antd.Button>
                        <span className="ant-divider"/>
                        <antd.Button title="Move down" icon="caret-down" size="default"
                                     onClick={() => this.moveDown(text.key)}></antd.Button>
                        <span className="ant-divider"/>
                        <antd.Button title="Run to this rule" icon="caret-right" size="default"
                                     onClick={() => this.runToHere(text.key)}></antd.Button>
                        <span className="ant-divider"/>
                        <antd.Button title="Delete" icon="close-circle" size="default"
                                     onClick={() => this.onDelete(text.key)}></antd.Button>
                    </span>
                )
            }]
        }
    },
    componentDidUpdate: function() {
        this.setJsonEditor(this.state.umsContent);
    },
    setJsonEditor: function(value) {
        var container = this.refs.umsContent;
        if(!container) return;
        if(!editor) {
            var options = {
                sortObjectKeys: true,
                search: false,
                mode: 'view',
                modes: ['code', 'view']
            };
            editor = new JSONEditor(container, options);
        }
        editor.set(value);
    },
    componentWillMount: function () {

        var self = this;
        var param = {
            tableId: this.props.location.state.passParam.tableId,
            dsId: this.props.location.state.passParam.dsId,
            dsType: this.props.location.state.passParam.dsType,
            groupId: this.props.location.state.passParam.groupId
        };

        $.get(utils.builPath("tables/getAllRules"), param, function (result) {
            utils.hideLoading();
            if (result.status != 200) {
                alert("Load rules or read kafka failed!");
                return;
            }

            var data = result.data.rules;
            for (var i = 0; i < data.length; i++) {
                data[i].key = i;
                data[i][RuleConst.rule.ruleGrammar] = JSON.parse(data[i][RuleConst.rule.ruleGrammar]);
            }

            self.setState({
                kafkaTopic: result.data.topic,
                kafkaOffset: result.data.offset - 20,
                kafkaCount: 20,
                rules: data
            });
        });
    },

    findOrderByKey: function (rules, key) {
        for (var i = 0; i < rules.length; i++) {
            if (rules[i].key == key) return i;
        }
        return -1;
    },

    onSelectChange: function (text, value) {
        var rules = JSON.parse(JSON.stringify(this.state.rules));
        var order = this.findOrderByKey(rules, text.key);
        text = rules[order];
        if (value == RuleConst.type.saveAs) {
            var ruleGrammar = [];
            var resultColumns = this.state.resultColumns;
            for (var i = 0; i < resultColumns.length; i++) {
                ruleGrammar.push(RuleConst.generateRuleGrammar());
                ruleGrammar[i][RuleConst.rule.ruleScope] = "" + i;
                ruleGrammar[i][RuleConst.rule.name] = "" + resultColumns[i].key;
            }
            text[RuleConst.rule.ruleGrammar] = ruleGrammar;
        } else if (value == RuleConst.type.toIndex) {
            var ruleGrammar = [];
            var resultColumns = this.state.resultColumns;
            for (var i = 0; i < resultColumns.length; i++) {
                ruleGrammar.push(RuleConst.generateRuleGrammar());
                ruleGrammar[i][RuleConst.rule.ruleScope] = "" + i;
                ruleGrammar[i][RuleConst.rule.ruleParamter] = resultColumns[i].key;
            }
            text[RuleConst.rule.ruleGrammar] = ruleGrammar;
        } else {
            text[RuleConst.rule.ruleGrammar] = [RuleConst.generateRuleGrammar()];
        }

        text[RuleConst.rule.ruleTypeName] = value;
        this.setState({
            rules: rules
        });
    },
    onRuleGrammarChange: function (rule) {
        var rules = [];
        for (var i = 0; i < this.state.rules.length; i++) {
            rules.push(this.state.rules[i]);
        }
        var order = this.findOrderByKey(rules, rule.key);
        rules[order] = rule;
        this.setState({
            rules: rules
        });
    },
    addRule: function () {
        var rules = this.state.rules;
        // 生成新的key值，取最小的没有被占用的自然数
        var generateNewKey = function (rules) {
            var result = 0;
            for (; ;) {
                var isExist = false;
                for (var i = 0; i < rules.length; i++) {
                    if (rules[i].key == result) {
                        isExist = true;
                        break;
                    }
                }
                if (isExist) {
                    result++;
                } else {
                    return result;
                }
            }
        };

        rules.push({
            groupId: this.props.location.state.passParam.groupId,
            ruleTypeName: RuleConst.type.select,
            ruleGrammar: [RuleConst.generateRuleGrammar()],
            key: generateNewKey(rules)
        });
        this.setState({
            rules: rules
        });
    },

    checkRuleLegal: function (rules, position) {
        if (position == null) position = 0;
        if (position >= rules.length) return true;

        var dsType = this.props.location.state.passParam.dsType;
        var curType = rules[position][RuleConst.rule.ruleTypeName];
        var preType = position > 0 ? rules[position - 1][RuleConst.rule.ruleTypeName] : null;
        /**
         * 第一条规则
         * 如果是UMS类型，那么第一条必须为flattenUms
         * 其他情况下，第一条必须为keyFilter或toIndex
         */
        if (preType == null) {
            if (dsType == utils.dsType.logUms) {
                if (curType != RuleConst.type.flattenUms) {
                    alert(dsType + "数据源的第一条规则必须是" + RuleConst.type.flattenUms);
                    return false;
                }
            } else if (dsType == utils.dsType.logLogstashJson
                || dsType == utils.dsType.logLogstash
                || dsType == utils.dsType.logFlume
                || dsType == utils.dsType.logFilebeat) {
                if (curType != RuleConst.type.keyFilter && curType != RuleConst.type.toIndex) {
                    alert(dsType + "数据源的第一条规则必须是" + RuleConst.type.keyFilter + "或" + RuleConst.type.toIndex);
                    return false;
                }
            } else {
                alert("不支持该数据源");
                return false;
            }
        }
        else {
            /**
             * 第二条和以后的规则
             * 总体规则顺序
             * flattenUms{0,1} keyFilter{0,} toIndex ...{0,} saveAs
             */
            if (curType == RuleConst.type.flattenUms) {
                alert(curType + "只能是第一条规则");
                return false;
            } else if (curType == RuleConst.type.keyFilter) {
                if (preType != RuleConst.type.flattenUms && preType != RuleConst.type.keyFilter) {
                    alert("第" + position + "条规则不能是" + curType);
                    return false;
                }
            } else if (curType == RuleConst.type.toIndex) {
                if (preType != RuleConst.type.flattenUms && preType != RuleConst.type.keyFilter) {
                    alert("第" + position + "条规则不能是" + curType);
                    return false;
                }
            } else if (curType == RuleConst.type.saveAs) {
                if (preType == RuleConst.type.flattenUms || preType == RuleConst.type.keyFilter || preType == RuleConst.type.saveAs) {
                    alert("第" + position + "条规则不能是" + curType);
                    return false;
                }
            } else {
                if (preType == RuleConst.type.flattenUms || preType == RuleConst.type.keyFilter || preType == RuleConst.type.saveAs) {
                    alert("第" + position + "条规则不能是" + curType);
                    return false;
                }
            }
        }

        if (!this.checkRuleLegal(rules, position + 1)) return false;

        return true;
    },

    saveAllRules: function () {

        var groupId = this.props.location.state.passParam.groupId;
        var rules = this.state.rules;
        if (rules.length == 0) {
            alert("必须有规则才能保存");
            return;
        }

        if (!this.checkRuleLegal(rules)) return;

        for (var i = 0; i < rules.length; i++) {
            rules[i][RuleConst.rule.orderId] = i;
        }
        var param = {
            groupId: groupId,
            rules: JSON.stringify(rules)
        };
        utils.showLoading();
        $.post(utils.builPath("tables/saveAllRules"), param, function (result) {
            utils.hideLoading();
            if (result.status != 200) {
                alert("Save rules failed!");
                return;
            }
            alert("Save rules success!");
        });
    },

    showData: function () {
        this.executeRules([]);
    },

    moveUp: function (key) {
        var rules = this.state.rules;
        var order = 0;
        for (var i = 0; i < rules.length; i++) {
            if (rules[i].key == key) {
                order = i;
                break;
            }
        }
        if (order == 0) return;

        [rules[order], rules[order - 1]] = [rules[order - 1], rules[order]];

        this.setState({rules: rules});
    },

    moveDown: function (key) {
        var rules = this.state.rules;
        var order = rules.length - 1;
        for (var i = 0; i < rules.length; i++) {
            if (rules[i].key == key) {
                order = i;
                break;
            }
        }
        if (order == rules.length - 1) return;

        [rules[order], rules[order + 1]] = [rules[order + 1], rules[order]];

        this.setState({rules: rules});
    },

    runToHere: function (key) {
        var rules = this.state.rules;
        var executeRules = [];
        for (var i = 0; i < rules.length; i++) {
            executeRules.push(rules[i]);
            if (rules[i].key == key) {
                break;
            }
        }
        this.executeRules(executeRules);
    },
    executeRules: function (executeRules) {

        if (!this.checkRuleLegal(executeRules)) return;

        var param = {
            kafkaTopic: this.state.kafkaTopic,
            kafkaOffset: this.state.kafkaOffset,
            kafkaCount: this.state.kafkaCount,
            executeRules: JSON.stringify(executeRules),
            dsType: this.props.location.state.passParam.dsType
        };
        const self = this;
        utils.showLoading();
        $.post(utils.builPath("tables/executeRules"), param, function (result) {
            utils.hideLoading();
            if (result.status != 200) {
                alert("Execute rules failed!");
                return;
            }

            var offset = result.data.offset;
            var data = result.data.data;
            var dataType = result.data.dataType;

            const DATA_TYPE = {
                UMS: 'UMS',
                FIELD: 'FIELD',
                JSON: 'JSON',
                STRING: 'STRING'
            };

            const generateResultSourceFromData = function (data, offset) {
                if (utils.isEmpty(data)) return null;
                var result = [];
                for (var i = 0; i < data.length; i++) {
                    var row = {
                        key: i,
                        offset: offset[i]
                    };
                    if (dataType == DATA_TYPE.UMS) {
                        row.ums = data[i][0];

                        var ums = JSON.parse(data[i][0]);

                        var schema = ums.schema;
                        row.namespace = schema.namespace;

                        var fields = schema.fields;
                        fields.forEach(function (field, index) {
                            if (index == 0) {
                                row.schema = field.name;
                            } else {
                                row.schema += ', ' + field.name;
                            }
                        });

                        var tuple = ums.payload[0].tuple;
                        tuple.forEach(function (value, index) {
                            if (index == 0) {
                                row.tuple = value;
                            } else {
                                row.tuple += ', ' + value;
                            }
                        });
                    }
                    else if (dataType == DATA_TYPE.FIELD) {
                        for (var j = 0; j < data[i].length; j++) {
                            row['col' + j] = JSON.parse(data[i][j]).value;
                        }
                    } else if (dataType == DATA_TYPE.JSON) {
                        var json = JSON.parse(data[i][0]);
                        for (var key in json) {
                            row[key] = json[key];
                        }
                    } else if (dataType == DATA_TYPE.STRING){
                        for (var j = 0; j < data[i].length; j++) {
                            row['col' + j] = data[i][j];
                        }
                    }

                    result.push(row);
                }
                return result;
            };

            const generateResultColumnFromData = function (data) {
                if (utils.isEmpty(data)) return null;

                var showCellData = function (words) {
                    if (words == null || words == "") return <span style={{color:"#D0D0D0"}}>{"(empty)"}</span>;
                    return words;
                };

                var result = [];
                if (dataType == DATA_TYPE.UMS) {
                    result.push({
                        title: 'namespace',
                        key: 'namespace',
                        render: (text) => (
                            <antd.Popover title={'namespace, offset='+text.offset}
                                          content={text.namespace}>
                                <span className="ant-tabs-nav-scroll">
                                    <a onClick={() => ((ums)=>{
                                        self.setState({
                                            visible: true,
                                            umsContent: JSON.parse(ums)
                                        });
                                    })(text.ums)}>{text.namespace}</a>
                                </span>
                            </antd.Popover>
                        ),
                        className: 'ant-table-long-td-min-width'
                    });
                    result.push({
                        title: 'schema',
                        key: 'schema',
                        render: (text) => (
                            <antd.Popover title={'schema, offset='+text.offset}
                                          content={text.schema}>
                                <span className="ant-tabs-nav-scroll" onDblClick={() => self.setState({
                                    detailVisible: true,
                                    detailTitle: 'schema, offset='+text.offset,
                                    detailContent: text.schema
                                })}>{text.schema}</span>
                            </antd.Popover>
                        ),
                        className: 'ant-table-long-td-min-width'
                    });
                    result.push({
                        title: 'tuple',
                        key: 'tuple',
                        render: (text) => (
                            <antd.Popover title={'tuple, offset='+text.offset}
                                          content={text.tuple}>
                                <span className="ant-tabs-nav-scroll" onDblClick={() => self.setState({
                                    detailVisible: true,
                                    detailTitle: 'tuple, offset='+text.offset,
                                    detailContent: text.tuple
                                })}>{text.tuple}</span>
                            </antd.Popover>
                        ),
                        className: 'ant-table-long-td-min-width'
                    });
                }
                else if (dataType == DATA_TYPE.FIELD) {
                    for (let i = 0; i < data[0].length; i++) {
                        let field = JSON.parse(data[0][i]);
                        let title = field[RuleConst.rule.name] + '(' + field[RuleConst.rule.type] + ')';
                        result.push({
                            title: title,
                            render: (text) => (
                                <antd.Popover
                                    title={title + ', offset='+text.offset}
                                    content={text['col'+i]}>
                                    <span className="ant-tabs-nav-scroll" onDoubleClick={() => self.setState({
                                        detailVisible: true,
                                        detailTitle: title + ', offset='+text.offset,
                                        detailContent: text['col'+i]
                                    })}>{showCellData(text['col' + i])}</span>
                                </antd.Popover>
                            ),
                            key: field[RuleConst.rule.name] + '(' + field[RuleConst.rule.type] + ')',
                            className: 'ant-table-long-td-min-width'
                        });
                    }
                } else if (dataType == DATA_TYPE.JSON) {
                    let keySet = new Set();
                    for (let i = 0; i < data.length; i++) {
                        let json = JSON.parse(data[i][0]);
                        for (let key in json) {
                            keySet.add(key);
                        }
                    }
                    keySet.forEach(function (key) {
                        result.push({
                            title: key,
                            render: (text) => (
                                <antd.Popover title={key + ', offset='+text.offset}
                                              content={text[key]}>
                                    <span className="ant-tabs-nav-scroll" onDoubleClick={() => self.setState({
                                        detailVisible: true,
                                        detailTitle: key + ', offset='+text.offset,
                                        detailContent: text[key]
                                    })}>{showCellData(text[key])}</span>
                                </antd.Popover>
                            ),
                            key: key,
                            className: 'ant-table-long-td-min-width'
                        })
                    });
                }
                else if (dataType == DATA_TYPE.STRING) {
                    let maxLen = 0;
                    for (let i = 0; i < data.length; i++) {
                        maxLen = Math.max(maxLen, data[i].length);
                    }
                    for (let i = 0; i < maxLen; i++) {
                        result.push({
                            title: i,
                            render: (text) => (
                                <antd.Popover title={'col_' + i + ', offset='+text.offset}
                                              content={text['col'+i]}>
                                    <span className="ant-tabs-nav-scroll" onDoubleClick={() => self.setState({
                                        detailVisible: true,
                                        detailTitle: 'col_' + i + ', offset='+text.offset,
                                        detailContent: text['col'+i]
                                    })}>{showCellData(text['col' + i])}</span>
                                </antd.Popover>
                            ),
                            key: 'col' + i,
                            className: 'ant-table-short-td-min-width'
                        });
                    }
                }
                return result;
            };

            self.setState({
                resultSource: generateResultSourceFromData(data, offset),
                resultColumns: generateResultColumnFromData(data)
            });
        });
    },
    onDelete: function (key) {
        var rules = this.state.rules;
        for (var i = 0; i < rules.length; i++) {
            if (rules[i].key == key) {
                rules.splice(i, 1);
                break;
            }
        }
        this.setState({
            rules: rules
        });
    },

    comeBack: function () {
        var passParam = {
            tableId: this.props.location.state.passParam.tableId,
            dsId: this.props.location.state.passParam.dsId,
            dsName: this.props.location.state.passParam.dsName,
            dsType: this.props.location.state.passParam.dsType,
            schemaName: this.props.location.state.passParam.schemaName,
            tableName: this.props.location.state.passParam.tableName
        };
        utils.showLoading();
        this.props.history.pushState({passParam: passParam}, "/data-table/config-rule");
    },

    render: function () {
        /**
         * 在渲染之前，赋值orderId
         */
        var rules = this.state.rules;
        for (var i = 0; i < rules.length; i++) {
            rules[i][RuleConst.rule.orderId] = i;
        }

        return (
            <div className="container-fluid">
                <div className="row header">
                    <h4 className="col-xs-3">
                        <antd.Button onClick={this.comeBack}>Back</antd.Button>
                    </h4>
                    <h4 className="col-xs-3">
                        <antd.Input addonBefore="Topic:"
                                    onChange={(event) => {this.setState({kafkaTopic: event.target.value})}}
                                    value={this.state.kafkaTopic}/>
                    </h4>
                    <h4 className="col-xs-3">
                        <antd.Input addonBefore="Offset:"
                                    onChange={(event) => {this.setState({kafkaOffset: event.target.value})}}
                                    value={this.state.kafkaOffset}/>
                    </h4>
                    <h4 className="col-xs-3">
                        <antd.Input addonBefore="Count:"
                                    onChange={(event) => {this.setState({kafkaCount: event.target.value})}}
                                    value={this.state.kafkaCount}/>
                    </h4>

                </div>
                <div className="row header">
                    <h4 className="col-xs-3">
                        {"Group name: " + this.props.location.state.passParam.groupName}
                    </h4>
                    <h4 className="col-xs-3">
                        {"Status: " + this.props.location.state.passParam.status}
                    </h4>
                    <h4 className="col-xs-6">
                        <div style={{float:"right"}}>
                            <antd.Button icon="save" type="primary" onClick={this.saveAllRules}>Save all rules
                            </antd.Button>
                            <span className="ant-divider"/>
                            <antd.Button icon="file-text" onClick={this.showData}>Show Data</antd.Button>
                            <span className="ant-divider"/>
                            <antd.Button icon="plus-circle" onClick={this.addRule}>Add</antd.Button>
                        </div>
                    </h4>

                    <div style={{overflowX:"auto",overflowY:"auto",width:"100%"}} className="col-xs-12">
                        <antd.Table locale={{emptyText:"No rule"}} bordered size="middle"
                                    pagination={false}
                                    dataSource={rules}
                                    columns={this.state.ruleColumns}></antd.Table>
                    </div>
                    <h4 className="col-xs-8">Result:</h4>
                    <ShouldUpdateAntdTable resultSource={this.state.resultSource} resultColumns={this.state.resultColumns}/>

                </div>

                <antd.Modal title="UMS" width={1280} visible={this.state.visible} onOk={() => this.setState({visible: false})} onCancel={() => this.setState({visible: false})}>
                    <div style={{height:768}} ref="umsContent"></div>
                </antd.Modal>

                <antd.Modal title={this.state.detailTitle} width={1280} visible={this.state.detailVisible} onOk={() => this.setState({detailVisible: false})} onCancel={() => this.setState({detailVisible: false})}>
                    <div style={{wordWrap: 'break-word'}}>{this.state.detailContent}</div>
                </antd.Modal>

            </div>
        );
    }
});

module.exports = EditRule;
