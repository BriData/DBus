import React, { PropTypes, Component } from 'react'
import { connect } from 'react-redux'
import { FormattedMessage } from 'react-intl'
import { createStructuredSelector } from 'reselect'
import Helmet from 'react-helmet'
import {message, Popover} from 'antd'
import Request from "@/app/utils/request"
import {intlMessage} from "@/app/i18n"
// 导入自定义组件
import {
  Bread,
  RuleEditorSearch,
  RuleEditorResultGrid,
  RuleEditorUmsModal,
  RuleEditorGrid
} from '@/app/components/index'
import {
  GET_ALL_RULES_API,
  EXECUTE_RULES_API,
  SAVE_ALL_RULES_API
} from './api'
// selectors
import { makeSelectLocale } from '../LanguageProvider/selectors'
// action



// 链接reducer和action
@connect(
  createStructuredSelector({
    locale: makeSelectLocale()
  }),
  dispatch => ({
  })
)
export default class RuleWrapper extends Component {
  constructor (props) {
    super(props)
    this.query = {}
    this.state = {
      // 用于传递到表单，以后就用不着了
      kafkaOffset: 0,
      kafkaCount: 20,
      kafkaTopic: null,

      umsModalVisible: false,
      umsModalUms: {},
      umsModalKey: 'umsModalKey',

      rules: [],
      resultSource: [],
      resultColumns: [],
    }
  }

  handleRandom = key =>
    `${Math.random()
      .toString(32)
      .substr(3, 8)}${key || ''}`

  componentWillMount () {

    const initialGetAllRules = () => {
      Request(GET_ALL_RULES_API, {
        params: {
          tableId: this.query.tableId,
          dsId: this.query.dsId,
          dsType: this.query.dsType,
          groupId: this.query.groupId
        },
        method: 'get'
      })
        .then(res => {
          if (res && res.status === 0) {
            let {rules, offset, topic} = res.payload
            if (offset >= 20) {
              offset -= 20
            }
            this.setState({
              rules: rules.map(rule => ({
                ...rule,
                isFold: true,
                ruleGrammar: JSON.parse(rule.ruleGrammar).map(grammar => ({
                  ...grammar, key: this.handleRandom('grammar')
                })),
                key: this.handleRandom('rule')
              })),
              kafkaTopic: topic,
              kafkaOffset: offset,
            }, () => this.handleExecuteRule([]))
          } else {
            message.warn(res.message)
          }
        })
        .catch(error => {
          error.response && error.response.data && error.response.data.message
            ? message.error(error.response.data.message)
            : message.error(error.message)
        })
    }

    const {location} = this.props
    const {query} = location
    if (Object.keys(query).length) {
      this.query = query
      initialGetAllRules()
    } else {
      window.location.href='/resource-manage/data-table'
    }
  }

  handleExecuteRule = (rules) => {
    const self = this

    const isEmpty = (obj) => {
      for (let name in obj) {
        if (obj.hasOwnProperty(name)) {
          return false;
        }
      }
      return true;
    }

    const generateResultSourceFromData = (data, offset, dataType) => {
      if (isEmpty(data)) return null
      let result = []
      for (let i = 0; i < data.length; i++) {
        let row = {
          key: i,
          offset: offset[i]
        }
        if (dataType === 'UMS') {
          row.ums = data[i][0]

          let ums = JSON.parse(data[i][0])

          let schema = ums.schema
          row.namespace = schema.namespace

          let fields = schema.fields
          fields.forEach(function (field, index) {
            if (index === 0) {
              row.schema = field.name
            } else {
              row.schema += ', ' + field.name
            }
          })

          let tuple = ums.payload[0].tuple
          tuple.forEach(function (value, index) {
            if (index === 0) {
              row.tuple = value
            } else {
              row.tuple += ', ' + value
            }
          })
        }
        else if (dataType === 'FIELD') {
          for (let j = 0; j < data[i].length; j++) {
            row['col' + j] = JSON.parse(data[i][j]).value
          }
        } else if (dataType === 'JSON') {
          let json = JSON.parse(data[i][0])
          for (let key in json) {
            row[key] = json[key]
          }
        } else if (dataType === 'STRING') {
          for (let j = 0; j < data[i].length; j++) {
            row['col' + j] = data[i][j]
          }
        }
        result.push(row)
      }
      return result
    }

    const generateResultColumnFromData = (data, offset, dataType) => {
      if (isEmpty(data)) return null

      let showCellData = function (words) {
        if (!words) return <span style={{color: "#D0D0D0"}}>{"(empty)"}</span>
        return words
      }

      let result = []
      if (dataType === 'UMS') {
        result.push({
          title: 'namespace',
          key: 'namespace',
          render: (text) => (
            <Popover title={'namespace, offset=' + text.offset}
                          content={text.namespace}>
                                <span className="ant-tabs-nav-scroll">
                                    <a onClick={() => this.handleOpenUmsModal(text.ums)}>{text.namespace}</a>
                                </span>
            </Popover>
          ),
          className: 'ant-table-long-td-min-width'
        })
        result.push({
          title: 'schema',
          key: 'schema',
          render: (text) => (
            <Popover title={'schema, offset=' + text.offset}
                          content={text.schema}>
                                <span className="ant-tabs-nav-scroll">{text.schema}</span>
            </Popover>
          ),
          className: 'ant-table-long-td-min-width'
        })
        result.push({
          title: 'tuple',
          key: 'tuple',
          render: (text) => (
            <Popover title={'tuple, offset=' + text.offset}
                          content={text.tuple}>
                                <span className="ant-tabs-nav-scroll" >{text.tuple}</span>
            </Popover>
          ),
          className: 'ant-table-long-td-min-width'
        })
      }
      else if (dataType === 'FIELD') {
        for (let i = 0; i < data[0].length; i++) {
          let field = JSON.parse(data[0][i])
          let title = field.name + '(' + field.type + ')'
          result.push({
            title: title,
            render: (text) => (
              <Popover
                title={title + ', offset=' + text.offset}
                content={text['col' + i]}>
                                    <span className="ant-tabs-nav-scroll" >{showCellData(text['col' + i])}</span>
              </Popover>
            ),
            key: field.name + '(' + field.type + ')',
            className: 'ant-table-long-td-min-width'
          })
        }
      } else if (dataType === 'JSON') {
        let keySet = new Set()
        for (let i = 0; i < data.length; i++) {
          let json = JSON.parse(data[i][0])
          for (let key in json) {
            keySet.add(key)
          }
        }
        keySet.forEach(function (key) {
          result.push({
            title: key,
            render: (text) => (
              <Popover title={key + ', offset=' + text.offset}
                            content={text[key]}>
                                    <span className="ant-tabs-nav-scroll" >{showCellData(text[key])}</span>
              </Popover>
            ),
            key: key,
            className: 'ant-table-long-td-min-width'
          })
        })
      }
      else if (dataType === 'STRING') {
        let maxLen = 0
        for (let i = 0; i < data.length; i++) {
          maxLen = Math.max(maxLen, data[i].length)
        }
        for (let i = 0; i < maxLen; i++) {
          result.push({
            title: i,
            render: (text) => (
              <Popover title={'col_' + i + ', offset=' + text.offset}
                            content={text['col' + i]}>
                                    <span className="ant-tabs-nav-scroll" >{showCellData(text['col' + i])}</span>
              </Popover>
            ),
            key: 'col' + i,
            className: 'ant-table-short-td-min-width'
          })
        }
      }
      return result
    }

    this.refs.searchRef.validateFields((err, values) => {
      if (err) return
      const {kafkaTopic,kafkaOffset, kafkaCount} = values
      Request(EXECUTE_RULES_API, {
        data: {
          kafkaTopic,
          kafkaOffset,
          kafkaCount,
          executeRules: JSON.stringify(rules),
          dsType: this.query.dsType
        },
        method: 'post'
      })
        .then(res => {
          if (res && res.status === 0) {
            const {data, offset, dataType} = res.payload
            self.setState({
              resultSource: generateResultSourceFromData(data, offset, dataType),
              resultColumns: generateResultColumnFromData(data, offset, dataType)
            }, () => {
              const {rules} = this.state
              if (rules.length) return
              // flattenUms
              if (this.query.dsType === 'log_ums') {
                rules.push({
                  groupId: this.query.groupId,
                  ruleTypeName: 'flattenUms',
                  ruleGrammar: [this.generateRuleGrammar()],
                  key: this.handleRandom('rule')
                })
              }
              // toIndex
              rules.push({
                groupId: this.query.groupId,
                ruleTypeName: 'toIndex',
                ruleGrammar: [this.generateRuleGrammar()],
                key: this.handleRandom('rule')
              })
              let text = rules[rules.length - 1];
              let ruleGrammar = [];
              const {resultColumns} = this.state
              for (let i = 0; i < resultColumns.length; i++) {
                ruleGrammar.push(this.generateRuleGrammar());
                ruleGrammar[i].ruleScope = "" + i;
                ruleGrammar[i].ruleParamter = resultColumns[i].key;
              }
              text.ruleGrammar = ruleGrammar;
              // saveAs
              rules.push({
                groupId: this.query.groupId,
                ruleTypeName: 'saveAs',
                ruleGrammar: [this.generateRuleGrammar()],
                key: this.handleRandom('rule')
              })
              text = rules[rules.length - 1];
              ruleGrammar = []
              for (let i = 0; i < resultColumns.length; i++) {
                ruleGrammar.push(this.generateRuleGrammar());
                ruleGrammar[i].ruleScope = "" + i;
                ruleGrammar[i].name = "" + resultColumns[i].key;
              }
              text.ruleGrammar = ruleGrammar;
              this.handleSetRules(rules)
              message.info('已自动生成规则')
            });
          } else {
            message.warn(res.message)
          }
        })
        .catch(error => {
          error.response.data && error.response.data.message
            ? message.error(error.response.data.message)
            : message.error(error.message)
        })
    })
  }

  handleOpenUmsModal = ums => {
    this.setState({
      umsModalVisible: true,
      umsModalUms: ums,
    })
  }

  handleCloseUmsModal = () => {
    this.setState({
      umsModalVisible: false,
      umsModalKey: this.handleRandom('umsModalKey')
    })
  }

  handleSetRules = rules => {
    this.setState({rules})
  }

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

  handleAddRule = () => {
    const {rules} = this.state
    rules.push({
      groupId: this.query.groupId,
      ruleTypeName: 'select',
      ruleGrammar: [this.generateRuleGrammar()],
      key: this.handleRandom('rule')
    })
    this.handleSetRules(rules)
  }

  checkRuleLegal = (rules, position) => {
    if (!position) position = 0;
    if (position >= rules.length) return true;

    let dsType = this.query.dsType;
    let curType = rules[position].ruleTypeName;
    let preType = position > 0 ? rules[position - 1].ruleTypeName : null;
    /**
     * 第一条规则
     * 如果是UMS类型，那么第一条必须为flattenUms
     * 其他情况下，第一条必须为keyFilter或toIndex
     */
    if (!preType) {
      if (dsType === 'log_ums') {
        if (curType !== 'flattenUms') {
          alert(dsType + "数据源的第一条规则必须是flattenUms");
          return false;
        }
      } else if (dsType === 'log_logstash_json'
        || dsType === 'log_logstash'
        || dsType === 'log_flume'
        || dsType === 'log_filebeat') {
        if (curType !== 'keyFilter' && curType !== 'toIndex') {
          alert(dsType + "数据源的第一条规则必须是keyFilter或toIndex");
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
      if (curType === 'flattenUms') {
        alert(curType + "只能是第一条规则");
        return false;
      } else if (curType === 'keyFilter') {
        if (preType !== 'flattenUms' && preType !== 'keyFilter') {
          alert("第" + position + "条规则不能是" + curType);
          return false;
        }
      } else if (curType === 'toIndex') {
        if (preType !== 'flattenUms' && preType !== 'keyFilter') {
          alert("第" + position + "条规则不能是" + curType);
          return false;
        }
      } else if (curType === 'saveAs') {
        if (preType === 'flattenUms' || preType === 'keyFilter' || preType === 'saveAs') {
          alert("第" + position + "条规则不能是" + curType);
          return false;
        }
      } else {
        if (preType === 'flattenUms' || preType === 'keyFilter' || preType === 'saveAs') {
          alert("第" + position + "条规则不能是" + curType);
          return false;
        }
      }
    }
    if (!this.checkRuleLegal(rules, position + 1)) return false;
    return true;
  }

  handleSaveAllRules = () => {
    let groupId = this.query.groupId
    const {rules} = this.state
    if (rules.length === 0) {
      alert("必须有规则才能保存");
      return;
    }

    if (!this.checkRuleLegal(rules)) return;

    for (let i = 0; i < rules.length; i++) {
      rules[i].orderId = i;
    }
    const data = {
      groupId: groupId,
      rules: JSON.stringify(rules)
    }
    Request(SAVE_ALL_RULES_API, {
      data: data,
      method: 'post'
    })
      .then(res => {
        if (res && res.status === 0) {
          message.success(res.message)
        } else {
          message.warn(res.message)
        }
      })
      .catch(error => {
        error.response.data && error.response.data.message
          ? message.error(error.response.data.message)
          : message.error(error.message)
      })
  }

  render () {
    const breadSource = [
      {
        path: '/resource-manage',
        name: 'home'
      },
      {
        path: '/resource-manage',
        name: '数据源管理'
      },
      {
        path: '/resource-manage/rule',
        name: '规则编辑'
      }
    ]
    const {kafkaOffset, kafkaCount, kafkaTopic} = this.state
    const {resultSource, resultColumns} = this.state
    const {umsModalKey, umsModalUms, umsModalVisible} = this.state
    const {rules} = this.state
    console.info('rules=',rules)
    return (
      <div>
        <Helmet
          title="Sink"
          meta={[{ name: 'description', content: 'Sink Manage' }]}
        />
        <Bread source={breadSource} />
        <RuleEditorSearch
          ref='searchRef'
          kafkaOffset={kafkaOffset}
          kafkaCount={kafkaCount}
          kafkaTopic={kafkaTopic}
          onAddRule={this.handleAddRule}
          onExecuteRule={this.handleExecuteRule}
          onSaveAllRules={this.handleSaveAllRules}
        />
        <h2><FormattedMessage
          id="app.components.resourceManage.rule.editRule"
          defaultMessage="编辑规则"
        />:</h2>
        <RuleEditorGrid
          query={this.query}
          rules={rules}
          resultColumns={resultColumns}
          onSetRules={this.handleSetRules}
          onExecuteRule={this.handleExecuteRule}
        />
        <h2><FormattedMessage
          id="app.components.resourceManage.rule.result"
          defaultMessage="运行结果"
        />:</h2>
        <RuleEditorResultGrid
          resultSource={resultSource}
          resultColumns={resultColumns}
        />
        <RuleEditorUmsModal
          visible={umsModalVisible}
          ums={umsModalUms}
          key={umsModalKey}
          onClose={this.handleCloseUmsModal}
        />
      </div>
    )
  }
}
RuleWrapper.propTypes = {
  locale: PropTypes.any,
  searchSinkList: PropTypes.func,
  setSearchSinkParam: PropTypes.func
}
