import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'
import {createStructuredSelector} from 'reselect'
import Helmet from 'react-helmet'
import {makeSelectLocale} from '../LanguageProvider/selectors'
import {Bread, GlobalConfigForm} from '@/app/components'
import {GlobalConfigModel, ZKManageModel} from './selectors'
import {initGlobalConf, readZkProperties, updateGlobalConf} from "./redux";

const ZK_PATH = '/DBus/Commons/global.properties'

// 链接reducer和action
@connect(
  createStructuredSelector({
    locale: makeSelectLocale(),
    ZKManageData: ZKManageModel(),
    GlobalConfigData: GlobalConfigModel()
  }),
  dispatch => ({
    readZkProperties: param => dispatch(readZkProperties.request(param)),
    updateGlobalConf: param => dispatch(updateGlobalConf.request(param)),
    initGlobalConf: param => dispatch(initGlobalConf.request(param)),
  })
)
export default class GlobalConfigWrapper extends Component {
  constructor(props) {
    super(props)
    this.state = {}
  }

  componentWillMount() {
    const {readZkProperties} = this.props
    readZkProperties({path: ZK_PATH})
  }

  handleSave = content => {
    const {updateGlobalConf} = this.props
    updateGlobalConf(content)
  }

  handleInit = (options, content) => {
    const {initGlobalConf} = this.props
    initGlobalConf({
      options,
      content
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
        path: '/config-manage/global-config',
        name: '全局配置'
      }
    ]
    const config = this.props.ZKManageData.zkProperties.result.payload
    return (
      <div>
        <Helmet
          title="数据源管理"
          meta={[
            {name: 'description', content: 'Description of DataSource Manage'}
          ]}
        />
        <Bread source={breadSource}/>
        <GlobalConfigForm
          config={config || {}}
          onSave={this.handleSave}
          onInit={this.handleInit}
        />
      </div>
    )
  }
}
GlobalConfigWrapper.propTypes = {
  locale: PropTypes.any
}
