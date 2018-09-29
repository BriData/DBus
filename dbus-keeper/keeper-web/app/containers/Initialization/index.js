/**
 * @author 戎晓伟
 * @description  注册Container组件
 */

import React, {PropTypes, Component} from 'react'
import {message} from 'antd'
import {connect} from 'react-redux'
import {createStructuredSelector} from 'reselect'
import Helmet from 'react-helmet'
// 导入样式
import styles from './res/styles/initialization.less'
// 导入自定义组件
import {InitializationForm, Foot} from '@/app/components'
import {InitializationModel} from './selectors'
import {getBasicConf} from './redux'
import {SAVE_BASIC_CONF_API} from './api'
import Request from "@/app/utils/request";
import {CHECK_INIT_API} from "@/app/containers/ConfigManage/api";

const INIT_ZOOKEEPER_ERROR = 17002;
const HEART_BEAT_SSH_ERROR = 17004;
const INFLUXDB_URL_IS_WRONG = 17005;
const KAFKA_BOOTSTRAP_SERVERS_IS_WRONG = 17006;
const MONITOR_URL_IS_WRONG = 17007;
const MONITOR_TOKEN_IS_WRONG = 17008
const STORM_NIMBUS_SERVE_IS_WRONG = 17009;
const MGR_IS_WRONG = 17010
const SINK_IS_WRONG = 17011
const SUPER_USER_ERROR = 17012
const ENCODE_PLUGIN_ERROR = 17013
const STORM_HOME_PATH_ERROR = 17014
const HEART_BEAT_PATH_ERROR = 17015
const STORM_UI_ERROR = 17016
@connect(
  createStructuredSelector({
    InitializationData: InitializationModel()
  }),
  dispatch => ({
    getBasicConf: params => dispatch(getBasicConf.request(params)),
  })
)
export default class InitializationWrapper extends Component {

  constructor (props) {
    super(props)
    this.state = {
      ...this.generateStateValidateStatus(''),
      loading: false,
    }
  }

  generateStateValidateStatus = value => {
    return {
      kafkaValidateStatus: value,
      grafanaValidateStatus: value,
      grafanaTokenValidateStatus: value,
      influxdbValidateStatus: value,
      stormValidateStatus: value,
      stormHomePathValidateStatus: value,
      stormUIValidateStatus: value,
      heartbeatValidateStatus: value,
      heartbeatPathValidateStatus: value,
    }
  }

  componentWillMount = () => {
    const {getBasicConf} = this.props
    getBasicConf()

    // this.checkInit()
  }

  checkInit = () => {
    Request(CHECK_INIT_API, {})
      .then(res => {
        if (res && res.status === 0) {
          if(res.payload) {
            message.error('请注意，Keeper已经初始化，2秒后自动跳转到登陆页面')
            setTimeout(() => this.props.router.push('/login'), 2000)
          }
        } else {
          message.error(res.message)
        }
      })
      .catch(error => {
        message.error('服务异常，可能是后台服务启动缓慢，请等待30秒后再尝试', 5)
      })
  }

  handleSave = data => {
    this.setState({
      ...this.generateStateValidateStatus('validating'),
      loading: true
    })
    Request(SAVE_BASIC_CONF_API, {
      data: data,
      method: 'post' })
      .then(res => {
        this.setState({
          ...this.generateStateValidateStatus(''),
          loading: false
        })
        if (res.status) message.error(res.message)
        switch (res.status) {
          case KAFKA_BOOTSTRAP_SERVERS_IS_WRONG:
            this.setState({
              kafkaValidateStatus: 'error'
            })
            break
          case MONITOR_URL_IS_WRONG:
            this.setState({
              grafanaValidateStatus: 'error'
            })
            break
          case MONITOR_TOKEN_IS_WRONG:
            this.setState({
              grafanaTokenValidateStatus: 'error'
            })
            break
          case STORM_NIMBUS_SERVE_IS_WRONG:
            this.setState({
              stormValidateStatus: 'error'
            })
            break
          case STORM_HOME_PATH_ERROR:
            this.setState({
              stormHomePathValidateStatus: 'error'
            })
            break
          case STORM_UI_ERROR:
            this.setState({
              stormUIValidateStatus: 'error'
            })
            break
          case INFLUXDB_URL_IS_WRONG:
            this.setState({
              influxdbValidateStatus: 'error'
            })
            break
          case HEART_BEAT_SSH_ERROR:
            this.setState({
              heartbeatValidateStatus: 'error'
            })
            break
          case HEART_BEAT_PATH_ERROR:
            this.setState({
              heartbeatPathValidateStatus: 'error'
            })
            break
          case 0:
            this.setState({
              ...this.generateStateValidateStatus('success')
            })
            message.success("初始化完成，3秒后将自动跳转到登录页面")
            setTimeout(() => this.props.router.push('/login'), 3000)
        }
      })
      .catch(error => {
        this.setState({
          loading: false,
        })
        message.error('服务器异常')
        error.response.data && error.response.data.message
          ? message.error(error.response.data.message)
          : message.error(error.message)
      })
  }

  render() {
    console.info(this.props)
    const basicConf = this.props.InitializationData.basicConf.result.payload || {}
    return (
      <div>
        <Helmet
          title="Initialization"
          meta={[{name: 'description', content: 'Description of Initialization'}]}
        />
        <div className={styles.registerContainer}>
          <div className={styles.header}>
            <div className={styles.layout}>
              <div className={styles.title}>欢迎使用DBus Keeper！开始前，请先进行基础配置。</div>
            </div>
          </div>
          <div className={styles.layout} style={{marginTop: '2%'}}>
            <div className={styles.layoutLeft}>
              <InitializationForm
                basicConf={basicConf}
                onSave={this.handleSave}
                state={this.state}
              />
            </div>
          </div>
        </div>
      </div>
    )
  }
}
InitializationWrapper.propTypes = {
  router: PropTypes.any
}
