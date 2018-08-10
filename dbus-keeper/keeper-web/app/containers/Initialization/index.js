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

const errorMessage = {
  17002: "请检查初始化配置,zk初始化异常",
  17004: "请检查心跳配置,心跳初始化异常",
  17005: "请检查inluxdb配置,inluxdb初始化异常",
  17006: "请检查kafka配置,kafka初始化异常",
  17007: "请检查grafana配置,grafana初始化异常",
  17008: "请检查grafana_token配置,前缀必须是Bearer",
  17009: "请检查storm配置,storm初始化异常",
  17010: "请检查mgr数据库配置,mgr数据库初始化异常",
  17011: "初始化默认sink异常",
  17012: "初始化默认super_user异常",
  17013: "初始化默认encode_plugin异常",
}
const INIT_ZOOKEEPER_ERROR = 17002;
const INIT_HEART_BEAT_ERROR = 17004;
const INFLUXDB_URL_IS_WRONG = 17005;
const KAFKA_BOOTSTRAP_SERVERS_IS_WRONG = 17006;
const MONITOR_URL_IS_WRONG = 17007;
const MONITOR_TOKEN_IS_WRONG = 17008
const STORM_NIMBUS_SERVE_IS_WRONG = 17009;
const MGR_IS_WRONG = 17010
const SINK_IS_WRONG = 17011
const SUPER_USER_ERROR = 17012
const ENCODE_PLUGIN_ERROR = 17013
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
      kafkaValidateStatus: '',
      grafanaValidateStatus: '',
      grafanaTokenValidateStatus: '',
      influxdbValidateStatus: '',
      stormValidateStatus: '',

      loading: false,
    }
  }

  componentWillMount = () => {
    const {getBasicConf} = this.props
    getBasicConf()

    this.checkInit()
  }

  checkInit = () => {
    Request(CHECK_INIT_API, {})
      .then(res => {
        if (res && res.status === 0) {
          if(res.payload) {
            message.error('请注意，Keeper已经初始化')
          }
        } else {
          message.error(res.message)
        }
      })
      .catch(error => {
        error.response.data && error.response.data.message
          ? message.error(error.response.data.message)
          : message.error(error.message)
      })
  }

  handleSave = data => {
    this.setState({
      kafkaValidateStatus: 'validating',
      grafanaValidateStatus: 'validating',
      grafanaTokenValidateStatus: 'validating',
      influxdbValidateStatus: 'validating',
      stormValidateStatus: 'validating',
      loading: true
    })
    Request(SAVE_BASIC_CONF_API, {
      data: data,
      method: 'post' })
      .then(res => {
        this.setState({
          loading: false,
          kafkaValidateStatus: '',
          grafanaValidateStatus: '',
          grafanaTokenValidateStatus: '',
          influxdbValidateStatus: '',
          stormValidateStatus: '',
        })
        if (res.status) message.error(errorMessage[res.status])
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
          case INFLUXDB_URL_IS_WRONG:
            this.setState({
              influxdbValidateStatus: 'error'
            })
            break
          case 0:
            this.setState({
              kafkaValidateStatus: 'success',
              grafanaValidateStatus: 'success',
              grafanaTokenValidateStatus: 'success',
              influxdbValidateStatus: 'success',
              stormValidateStatus: 'success',
            })
            message.success("初始化完成，3秒后将自动跳转到登录页面")
            setTimeout(() => this.props.router.push('/login'), 3000)
        }
      })
      .catch(error => {
        this.setState({
          loading: false,
        })
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
