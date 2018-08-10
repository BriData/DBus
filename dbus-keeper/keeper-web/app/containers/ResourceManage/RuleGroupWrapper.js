import React, { PropTypes, Component } from 'react'
import { connect } from 'react-redux'
import { createStructuredSelector } from 'reselect'
import Helmet from 'react-helmet'
import {message} from 'antd'
import dateFormat from 'dateformat'
// 导入自定义组件
import {
  Bread,
  RuleGroupSearch,
  RuleGroupGrid,
  RuleGroupRenameModal,
  RuleGroupCloneModal,
  RuleGroupAddGroupModal,
  RuleGroupDiffModal
} from '@/app/components/index'

// selectors
import {RuleGroupModel} from './selectors'
import { makeSelectLocale } from '../LanguageProvider/selectors'
// action
import {searchRuleGroup} from './redux'
import {intlMessage} from "@/app/i18n";
import Request from "@/app/utils/request";
import {
  ADD_RULE_GROUP_API,
  CLONE_RULE_GROUP_API,
  DELETE_RULE_GROUP_API,
  UPDATE_RULE_GROUP_API,
  UPGRADE_RULE_GROUP_API,
  DIFF_RULE_GROUP_API
} from './api'
import {
  SEND_CONTROL_MESSAGE_API
} from '../toolSet/api'
// 链接reducer和action
@connect(
  createStructuredSelector({
    RuleGroupData: RuleGroupModel(),
    locale: makeSelectLocale()
  }),
  dispatch => ({
    searchRuleGroup: param => dispatch(searchRuleGroup.request(param)),
  })
)
export default class RuleGroupWrapper extends Component {
  constructor (props) {
    super(props)
    this.query = {}
    this.selectedRows = []

    this.state = {
      newModalKey: 'newModalKey',
      newModalVisible: false,

      cloneModalKey: 'cloneModalKey',
      cloneModalVisible: false,
      cloneModalRecord: {},

      renameModalKey: 'renameModalKey',
      renameModalVisible: false,
      renameModalRecord: {},

      upgradeLoading: false,

      diffVisible: false,
      title: [],
      subTitle: [],
      content: []
    }
  }


  /**
   * @param key 传入一个key type:[Object String]  默认:空
   * @returns String 返回一个随机字符串
   */
  handleRandom = key =>
    `${Math.random()
      .toString(32)
      .substr(3, 8)}${key || ''}`;

  componentWillMount () {
    const {location} = this.props
    const {query} = location
    if (Object.keys(query).length) {
      this.query = query
    } else {
      window.location.href='/resource-manage/data-table'
    }
    // 初始化查询
    this.handleSearch()
  }

  /**
   * @param params 查询的参数 type:[Object Object]
   * @description 查询Sink列表
   */
  handleSearch = () => {
    const {searchRuleGroup} = this.props
    searchRuleGroup({tableId: this.query.tableId})
  }

  handleSelectChange = (selectedRowKeys, selectedRows) => {
    this.selectedRows = selectedRows
  }

  handleEditRuleGroup = record => {
    this.props.router.push({
      pathname: '/resource-manage/rule',
      query: {
        ...this.query,
        groupId: record.id
      }
    })
  }

  handleDiff = () => {
    Request(`${DIFF_RULE_GROUP_API}/${this.query.tableId}`, {
      method: 'get'
    })
      .then(res => {
        if (res && res.status === 0) {
          this.handleOpenDiffModal(res.payload)
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

  handleOpenDiffModal = data => {
    data = data.filter(d => {
      return this.selectedRows.some(row => row.groupName === d.groupName)
    })

    let title = data.map(function (d) {
      return d.groupName;
    });

    let subTitle = ['name', 'type'];

    let content = data.map(function (d) {
      let grammars = d.ruleGrammar;
      if(!grammars) return [];
      let ret = [];
      grammars = JSON.parse(grammars);
      grammars.forEach(function (grammar) {
        ret[grammar.ruleScope - 0] = [grammar.name, grammar.type];
      });
      return ret;
    });

    this.setState({
      diffVisible: true,
      title: title,
      subTitle: subTitle,
      content: content
    })
  }

  handleCloseDiffModal = () => {
    this.setState({
      diffVisible: false
    })
  }

  handleUpgradeVersion = () => {
    this.setState({upgradeLoading: true})
    Request(UPGRADE_RULE_GROUP_API, {
      params: {
        tableId: this.query.tableId,
        dsId: this.query.dsId,
        dsName: this.query.dsName,
        schemaName: this.query.schemaName,
        tableName: this.query.tableName
      },
      method: 'get'
    })
      .then(res => {
        if (res && res.status === 0) {
          this.handleAutoReloadLogProcessor()
        } else {
          message.warn(res.message)
          this.setState({upgradeLoading: false})
        }
      })
      .catch(error => {
        error.response && error.response.data && error.response.data.message
          ? message.error(error.response.data.message)
          : message.error(error.message)
        this.setState({upgradeLoading: false})
      })
  }

  handleAutoReloadLogProcessor = () => {
    const date = new Date()
    Request(SEND_CONTROL_MESSAGE_API, {
      data: {
        topic: this.query.ctrlTopic,
        message: JSON.stringify({
          from: 'dbus-web',
          id: date.getTime(),
          payload: {
            dsName: this.query.dsName,
            dsType: this.query.dsType
          },
          timestamp: dateFormat(date, 'yyyy-mm-dd HH:MM:ss.l'),
          type: 'LOG_PROCESSOR_RELOAD_CONFIG'
        })
      },
      method: 'post'
    })
      .then(res => {
        if (res && res.status === 0) {
          message.success(res.message)
        } else {
          message.warn(res.message)
        }
        this.setState({upgradeLoading: false})
      })
      .catch(error => {
        error.response && error.response.data && error.response.data.message
          ? message.error(error.response.data.message)
          : message.error(error.message)
        this.setState({upgradeLoading: false})
      })
  }

  handleOpenNewModal = () => {
    this.setState({
      newModalVisible: true,
    })
  }

  handleCloseNewModal = () => {
    this.setState({
      newModalVisible: false,
      newModalKey: this.handleRandom('new')
    })
  }

  handleOpenCloneModal = (record) => {
    this.setState({
      cloneModalVisible: true,
      cloneModalRecord: record
    })
  }

  handleCloseCloneModal = () => {
    this.setState({
      cloneModalVisible: false,
      cloneModalKey: this.handleRandom('clone')
    })
  }

  handleOpenRenameModal = (record) => {
    this.setState({
      renameModalVisible: true,
      renameModalRecord: record
    })
  }

  handleCloseRenameModal = () => {
    this.setState({
      renameModalVisible: false,
      renameModalKey: this.handleRandom('rename')
    })
  }

  handleRequest = (obj) => {
    const {api, params, data, method, callback, callbackParams} = obj
    Request(api, {
      params: {
        ...params
      },
      data: {
        ...data
      },
      method: method || 'get'
    })
      .then(res => {
        if (res && res.status === 0) {
          if (callback) {
            if (callbackParams) callback(...callbackParams)
            else callback()
          }
          this.handleSearch()
          message.success(res.message)
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

  render () {
    console.info(this.props)
    const {RuleGroupData} = this.props
    const {ruleGroups} = RuleGroupData

    const {newModalKey, newModalVisible} = this.state
    const {cloneModalKey, cloneModalVisible, cloneModalRecord} = this.state
    const {renameModalKey, renameModalVisible, renameModalRecord} = this.state
    const {diffVisible, title, subTitle, content} = this.state
    const {
      locale,
    } = this.props
    const localeMessage = intlMessage(locale)
    const {upgradeLoading} = this.state
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
        name: '规则组管理'
      },
      {
        name: this.query.dsName
      },
      {
        name: this.query.schemaName
      },
      {
        name: this.query.tableName
      }
    ]
    return (
      <div>
        <Helmet
          title="Rule Group"
          meta={[{ name: 'description', content: 'Rule Group' }]}
        />
        <Bread source={breadSource} />
        <RuleGroupSearch
          onOpenNewModal={this.handleOpenNewModal}
          onDiff={this.handleDiff}
          onUpgradeVersion={this.handleUpgradeVersion}
          upgradeLoading={upgradeLoading}
        />
        <RuleGroupGrid
          ruleGroups={ruleGroups}
          onSelectChange={this.handleSelectChange}
          onRequest={this.handleRequest}
          deleteApi={DELETE_RULE_GROUP_API}
          updateApi={UPDATE_RULE_GROUP_API}
          tableId={this.query.tableId}
          onOpenCloneModal={this.handleOpenCloneModal}
          onOpenRenameModal={this.handleOpenRenameModal}
          onEditRuleGroup={this.handleEditRuleGroup}
        />
        <RuleGroupRenameModal
          tableId={this.query.tableId}
          key={renameModalKey}
          visible={renameModalVisible}
          record={renameModalRecord}
          onClose={this.handleCloseRenameModal}
          onRequest={this.handleRequest}
          updateApi={UPDATE_RULE_GROUP_API}
        />
        <RuleGroupCloneModal
          tableId={this.query.tableId}
          key={cloneModalKey}
          visible={cloneModalVisible}
          record={cloneModalRecord}
          onClose={this.handleCloseCloneModal}
          onRequest={this.handleRequest}
          cloneApi={CLONE_RULE_GROUP_API}
        />
        <RuleGroupAddGroupModal
          tableId={this.query.tableId}
          key={newModalKey}
          visible={newModalVisible}
          onRequest={this.handleRequest}
          onClose={this.handleCloseNewModal}
          addApi={ADD_RULE_GROUP_API}
        />
        <RuleGroupDiffModal
          diffVisible={diffVisible}
          title={title}
          subTitle={subTitle}
          content={content}
          onClose={this.handleCloseDiffModal}
        />
      </div>
    )
  }
}
RuleGroupWrapper.propTypes = {
  locale: PropTypes.any,
}
