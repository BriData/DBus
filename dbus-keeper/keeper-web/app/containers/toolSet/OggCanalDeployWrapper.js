import React, {Component} from 'react'
import {connect} from 'react-redux'
import {createStructuredSelector} from 'reselect'
import {message} from 'antd'
// import Helmet from 'react-helmet'
// 导入自定义组件
import {OggCanalDeployForm, OggCanalDeployModifyModal, OggCanalDeploySearch} from '@/app/components'
import {makeSelectLocale} from '../LanguageProvider/selectors'
import {GET_OGG_CANAL_DEPLOY_INFO, SYNC_OGG_CANAL_DEPLOY_INFO} from './api'
import Request from "@/app/utils/request";
// 链接reducer和action
@connect(
  createStructuredSelector({
    locale: makeSelectLocale()
  }),
  dispatch => ({})
)
export default class OggCanalDeployWrapper extends Component {
  constructor(props) {
    super(props)
    this.state = {
      deployInfos: null,
      modalVisible: false,
      modalRecord: {},
      modalKey: 'modalKey',

      loading: false
    }
  }

  componentWillMount() {
    this.handleGetDeployInfo()
  }

  handleGetDeployInfo = () => {
    Request(GET_OGG_CANAL_DEPLOY_INFO, {
      method: 'get'
    })
      .then(res => {
        if (res && res.status === 0) {
          this.setState({
            deployInfos: res.payload
          })
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

  /**
   * @param key 传入一个key type:[Object String]  默认:空
   * @returns 返回一个随机字符串
   */
  handleRandom = key =>
    `${Math.random()
      .toString(32)
      .substr(3, 8)}${key || ''}`;

  handleCloseModifyModal = () => {
    this.handleGetDeployInfo()
    this.setState({
      modalVisible: false
    })
  }

  handleOpenModifyModal = (record) => {
    this.setState({
      modalKey: this.handleRandom("modalKey"),
      modalRecord: record,
      modalVisible: true
    })
  }

  handleSync = () => {
    this.setState({loading: true})
    Request(SYNC_OGG_CANAL_DEPLOY_INFO, {
      method: 'get'
    })
      .then(res => {
        if (res && res.status === 0) {
          this.handleGetDeployInfo()
        } else {
          message.warn(res.message)
        }
      })
      .catch(error => {
        error.response && error.response.data && error.response.data.message
          ? message.error(error.response.data.message)
          : message.error(error.message)
      })
    this.setState({loading: false})
  }

  render() {
    const {deployInfos, loading} = this.state
    const {modalKey, modalVisible, modalRecord} = this.state
    return (
      <div>
        <OggCanalDeploySearch
          onSearch={this.handleGetDeployInfo}
          onSync={this.handleSync}
          loading={loading}
        />
        <OggCanalDeployForm
          info={deployInfos}
          onModify={this.handleOpenModifyModal}
        />
        <OggCanalDeployModifyModal
          key={modalKey}
          visible={modalVisible}
          deployInfo={modalRecord}
          onClose={this.handleCloseModifyModal}
        />
      </div>
    )
  }
}
OggCanalDeployWrapper.propTypes = {}
