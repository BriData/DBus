import React, {PropTypes, Component} from 'react'
import {Popconfirm, Button, Select, Input, message, Popover, Icon, Table, Switch} from 'antd'
import {FormattedMessage} from 'react-intl'
import OperatingButton from '@/app/components/common/OperatingButton'
import EditRuleGrammar from './EditRuleGrammar'
// 导入样式
import styles from './res/styles/index.less'
const Option = Select.Option


var RuleTypeTips = {};

RuleTypeTips.flattenUms = (
  <div>
    <p>将UMS消息中的所有数据提取出来</p>
    <p>每个tuple作为单独一行数据</p>
  </div>
);
RuleTypeTips.toIndex = (
  <div>
    <p>将JSON源数据按Key映射为下标</p>
    <p>下标从小到大排序,从0开始</p>
  </div>
);
RuleTypeTips.filter = (
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
RuleTypeTips.trim = (
  <div>
    <p>Field为1，Operate为both，Parameter为abc</p>
    <p>trim第1列内容，abcXXXXXcab 变为 XXXXX</p>
  </div>
);
RuleTypeTips.replace = (
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
RuleTypeTips.split = (
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
RuleTypeTips.subString = (
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
RuleTypeTips.concat = (
  <div>
    <p>Field为1，3，5，-1，Parameter为#</p>
    <p>把1、3、5、最后一列用#链接</p>
    <p>eg.aa#bb#cc#dd</p>
  </div>
);
RuleTypeTips.select = (
  <div>
    <p>Field为1，3，5，-1</p>
    <p>从所有列中提取出第1、3、最后一列</p>
  </div>
);
RuleTypeTips.saveAs = (
  <div>
    <p>Field为1, name为"时间"，type为"DATETIME"</p>
    <p>指定第1列的名称为：时间，类型为：DATETIME</p>
  </div>
);
RuleTypeTips.prefixOrAppend = (
  <div>
    <p>Field为1, Operator为"after"，parameter为"aaa"</p>
    <p>在第一列的前面拼接上常量字符串"aaa"，第一列原始数据为123456，结果为aaa123456</p>
  </div>
);
RuleTypeTips.regexExtract = (
  <div>
    <p>'Field为1, parameter为(\d&#123;4&#125;年\d&#123;1,2&#125;月)已结清'</p>
    <p>从第1列中提取出****年**月</p>
    <p>eg. 2017年11月已结清</p>
    <p>提取结果：2017年11月</p>
  </div>
);

RuleTypeTips.jsonPath = (
  <div>
    <p>从JSON中按key提取value</p>
    <p>Field指定列，parameter指定要提取的Key</p>
    <p>例如parameter为$['version']，表示提取XXX.version</p>
    <p>如果需要提取深层的值，parameter可以填写为$['level1']['level2']['level3']，或为$['level1.level2.level3']</p>
    <p>如果需要提取数组中的某一项，parameter可以填写为$['data'][0]，表示提取XXX.data[0]</p>
  </div>
);

RuleTypeTips.flattenJsonArray = (
  <div>
    <p>提取指定路径下的JSON数组</p>
  </div>
);

export default class RuleEditorGrid extends Component {
  constructor(props) {
    super(props)
    this.state = {}
  }

  handleRandom = key =>
    `${Math.random()
      .toString(32)
      .substr(3, 8)}${key || ''}`

  generateRuleGrammar = () => ({
    key: this.handleRandom('grammar'),
    ruleParamter: null,
    ruleOperate: null,
    ruleScope: null,
    name: null,
    type: null,
    ruleType: null,
    subStartType: null,
    subStart: null,
    subEndType: null,
    subEnd: null,
    filterKey: null
  })

  findOrderByKey = (rules, key) => {
    for (let i = 0; i < rules.length; i++) {
      if (rules[i].key === key) return i;
    }
    return -1;
  }

  handleSelectChange = (text, value) => {
    const {rules} = this.props
    let order = this.findOrderByKey(rules, text.key);
    text = rules[order];
    if (value === 'saveAs') {
      const ruleGrammar = [];
      const {resultColumns} = this.props
      for (let i = 0; i < resultColumns.length; i++) {
        ruleGrammar.push(this.generateRuleGrammar());
        ruleGrammar[i].ruleScope = "" + i;
        ruleGrammar[i].name = "" + resultColumns[i].key;
      }
      text.ruleGrammar = ruleGrammar;
    } else if (value === 'toIndex') {
      const ruleGrammar = [];
      const {resultColumns} = this.props
      for (let i = 0; i < resultColumns.length; i++) {
        ruleGrammar.push(this.generateRuleGrammar());
        ruleGrammar[i].ruleScope = "" + i;
        ruleGrammar[i].ruleParamter = resultColumns[i].key;
      }
      text.ruleGrammar = ruleGrammar;
    } else {
      text.ruleGrammar = [this.generateRuleGrammar()];
    }
    text.ruleTypeName = value;
    const {onSetRules} = this.props
    onSetRules(rules)
  }

  handleFold = (fold, text) => {
    const {rules} = this.props;
    let order = this.findOrderByKey(rules, text.key);
    rules[order].isFold = fold;
    const {onSetRules} = this.props
    onSetRules(rules)
  }

  handleRuleGrammarChange = (rule) => {
    const {rules} = this.props;
    let order = this.findOrderByKey(rules, rule.key);
    rules[order] = rule;
    const {onSetRules} = this.props
    onSetRules(rules)
  }

  moveUp = (key) => {
    const {rules} = this.props
    let order = this.findOrderByKey(rules, key)
    if (order === 0) return;
    [rules[order], rules[order - 1]] = [rules[order - 1], rules[order]];
    const {onSetRules} = this.props
    onSetRules(rules)
  }

  moveDown = (key) => {
    const {rules} = this.props
    let order = this.findOrderByKey(rules, key)
    if (order === rules.length - 1) return;
    [rules[order], rules[order + 1]] = [rules[order + 1], rules[order]];
    const {onSetRules} = this.props
    onSetRules(rules)
  }

  runToHere = (key) => {
    const {rules} = this.props
    const executeRules = [];
    for (let i = 0; i < rules.length; i++) {
      executeRules.push(rules[i]);
      if (rules[i].key === key) {
        break;
      }
    }
    const {onExecuteRule} = this.props
    onExecuteRule(executeRules);
  }

  onDelete = (key) => {
    const {rules} = this.props
    for (let i = 0; i < rules.length; i++) {
      if (rules[i].key === key) {
        rules.splice(i, 1);
        break;
      }
    }
    const {onSetRules} = this.props
    onSetRules(rules)
  }

  render() {

    const rules = this.props.rules.map((rule, i) => ({...rule, orderId: i}))
    const columns = [{
      title: <FormattedMessage
        id="app.common.order"
        defaultMessage="编号"
      />,
      key: 'orderId',
      dataIndex: 'orderId',
      width: 50,
    }, {
      title: <FormattedMessage
        id="app.components.resourceManage.rule.ruleType"
        defaultMessage="规则类型"
      />,
      key: 'ruleTypeName',
      width: 200,
      render: (text) => (
        <span>
        <span className="ant-input-group-wrapper">
          <span className="ant-input-wrapper ant-input-group">
            <Select value={text.ruleTypeName} style={{width: 120}}
                    onChange={(value) => this.handleSelectChange(text, value)}>
            <Option value={'flattenUms'} disabled={this.props.query.dsType !== 'log_ums'}>{'flattenUms'}</Option>
            <Option value={'keyFilter'}>{'keyFilter'}</Option>
            <Option value={'toIndex'}>{'toIndex'}</Option>
            <Option value={'filter'}>{'filter'}</Option>
            <Option value={'trim'}>{'trim'}</Option>
            <Option value={'replace'}>{'replace'}</Option>
            <Option value={'split'}>{'split'}</Option>
            <Option value={'subString'}>{'subString'}</Option>
            <Option value={'concat'}>{'concat'}</Option>
            <Option value={'select'}>{'select'}</Option>
            <Option value={'saveAs'}>{'saveAs'}</Option>
            <Option value={'prefixOrAppend'}>{'prefixOrAppend'}</Option>
            <Option value={'regexExtract'}>{'regexExtract'}</Option>
            <Option value={'jsonPath'}>{'jsonPath'}</Option>
            <Option value={'flattenJsonArray'}>{'flattenJsonArray'}</Option>
            </Select>
            <Popover content={RuleTypeTips[text.ruleTypeName]}
                     title={text.ruleTypeName}>
              <Icon style={{marginLeft: 5}} type="question-circle-o"/>
            </Popover>
            {text.isFold ?
              (<span title={'展开'} style={{cursor: 'pointer'}}><Icon onClick={() => this.handleFold(false, text)} style={{marginLeft: 10}} type="down"/></span>) :
              (<span title={'收缩'} style={{cursor: 'pointer'}}><Icon onClick={() => this.handleFold(true, text)} style={{marginLeft: 10}} type="up"/></span>)
            }
          </span>
         </span>
    </span>
      )
    }, {
      title: <FormattedMessage
        id="app.components.resourceManage.rule.ruleArgs"
        defaultMessage="规则参数"
      />,
      key: 'ruleGrammar',
      render: (text) => (text.isFold ?
        (<Popover
          title={'ruleGrammar'}
          content={JSON.stringify(text.ruleGrammar)}
        >
          <div className={styles.ellipsis}>{JSON.stringify(text.ruleGrammar)}</div>
        </Popover>) :
        (<EditRuleGrammar onRuleGrammarChange={this.handleRuleGrammarChange} rule={text}/>)
      )
    }, {
      title: <FormattedMessage
        id="app.common.operate"
        defaultMessage="操作"
      />,
      key: "operation",
      width: 180,
      render: (text) => (
        <span>
          <Button shape="circle" title="Move up" icon="arrow-up" size="default"
                       onClick={() => this.moveUp(text.key)}/>
          <span className="ant-divider"/>
          <Button shape="circle" title="Move down" icon="arrow-down" size="default"
                       onClick={() => this.moveDown(text.key)}/>
          <span className="ant-divider"/>
          <Button shape="circle" title="Run to this rule" icon="caret-right" size="default"
                       onClick={() => this.runToHere(text.key)}/>
          <span className="ant-divider"/>
          <Button shape="circle" title="Delete" icon="close" size="default"
                       onClick={() => this.onDelete(text.key)}/>
        </span>
      )
    }]
    return (
      <div className={styles.table}>
        <Table
          className='rule-edit-grid'
          locale={{emptyText: "No rule"}}
          bordered
          size="middle"
          pagination={false}
          dataSource={rules}
          columns={columns}
        />
      </div>
    )
  }
}

RuleEditorGrid.propTypes = {}
