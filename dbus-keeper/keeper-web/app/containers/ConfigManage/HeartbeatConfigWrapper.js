import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'
import {createStructuredSelector} from 'reselect'
import Helmet from 'react-helmet'
import {makeSelectLocale} from '../LanguageProvider/selectors'
import {message} from 'antd'
import {Bread, HeartbeatConfigTabs} from '@/app/components'
import Request from "@/app/utils/request";
import {ZKManageModel} from './selectors'
import {DataSchemaModel} from '../ResourceManage/selectors'
import {SEND_MAIL_TEST_API} from './api/index.js'
import {readZkData, saveZkData} from "./redux"
import {searchAllDataSchema} from '../ResourceManage/redux'

const ZK_PATH = '/DBus/HeartBeat/Config/heartbeat_config.json'

// 链接reducer和action
@connect(
  createStructuredSelector({
    locale: makeSelectLocale(),
    ZKManageData: ZKManageModel(),
    DataSchemaData: DataSchemaModel(),
  }),
  dispatch => ({
    readZkData: param => dispatch(readZkData.request(param)),
    saveZkData: param => dispatch(saveZkData.request(param)),
    searchAllDataSchema: param => dispatch(searchAllDataSchema.request(param)),
  })
)
export default class HeartbeatConfigWrapper extends Component {
  constructor(props) {
    super(props)
    this.state = {
      config: null
    }
  }

  componentWillMount() {
    const {readZkData, searchAllDataSchema} = this.props
    readZkData({path: ZK_PATH})
    searchAllDataSchema()
  }

  componentWillReceiveProps = nextProps => {
    const content = (nextProps.ZKManageData.zkData.result.payload || {}).content
    const {config} = this.state
    content && !config && this.setState({
      config: JSON.parse(content)
    })
  }

  handleSave = content => {
    const {saveZkData} = this.props
    saveZkData({
      path: ZK_PATH,
      content: content
    })
    this.setState({
      config: JSON.parse(content)
    })
  }

  handleSendMailTest = content => {
    Request(SEND_MAIL_TEST_API, {
      data: JSON.parse(content),
      method: 'post'
    })
      .then(res => {
        if (res && res.status === 0 && res.payload) {
          message.success()
        } else {
          message.warn("请检查邮件配置")
        }
      })
      .catch(error => {
        error.response && error.response.data && error.response.data.message
          ? message.error(error.response.data.message)
          : message.error(error.message)
      })
  }

  render() {
    const breadSource = [
      {
        path: '/config-manage',
        name: 'home'
      },
      {
        path: '/config-manage',
        name: '配置中心'
      },
      {
        path: '/config-manage/heartbeat-config',
        name: '心跳配置'
      }
    ]
    const config = this.state.config || {}
    console.info(this.props)
    const allDataSchemaList = Object.values(this.props.DataSchemaData.allDataSchemaList.result)
    return (
      <div>
        <Helmet
          title="数据源管理"
          meta={[
            {name: 'description', content: 'Description of DataSource Manage'}
          ]}
        />
        <Bread source={breadSource}/>
        <HeartbeatConfigTabs
          config={config}
          allDataSchemaList={allDataSchemaList}
          onSave={this.handleSave}
          onSendMailTest={this.handleSendMailTest}
        />
      </div>
    )
  }
}
HeartbeatConfigWrapper.propTypes = {
  locale: PropTypes.any
}
