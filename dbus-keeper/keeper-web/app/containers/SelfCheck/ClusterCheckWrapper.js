import React, {PropTypes, Component} from 'react'
import {connect} from 'react-redux'
import {createStructuredSelector} from 'reselect'
import {message} from 'antd'

// import Helmet from 'react-helmet'
// 导入自定义组件
import {
  ClusterCheckForm
} from '@/app/components'
import {makeSelectLocale} from '../LanguageProvider/selectors'
import {} from '@/app/containers/SelfCheck/redux'
import {
  CHECK_CLUSTER_API
} from './api'
import Request from '@/app/utils/request'
// 链接reducer和action
@connect(
  createStructuredSelector({
    locale: makeSelectLocale()
  }),
  dispatch => ({})
)
export default class ClusterCheckWrapper extends Component {
  constructor (props) {
    super(props)
    this.state = {
      grafanaUrl: 'notok',
      influxdbUrl: 'notok',
      heartBeatLeader: [],
      kafkaBrokers: [],
      nimbuses: [],
      stormSSHSecretFree: 'notok',
      supervisors: [],
      zkStats: [],
      loading: false
    }
  }

  componentWillMount () {
    this.handleCheckCluster()
  }

  handleCheckCluster = (params) => {
    this.setState({loading: true})
    Request(CHECK_CLUSTER_API, {
      params: params,
      method: 'get'
    })
      .then(res => {
        if (res && res.status === 0) {
          this.setState({
            ...res.payload,
            loading: false
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

  render () {
    console.info(this.props)
    return (
      <div>
        <ClusterCheckForm
          onCheckCluster={this.handleCheckCluster}
          checkResult={this.state}
        />
      </div>
    )
  }
}
ClusterCheckWrapper.propTypes = {
  locale: PropTypes.any
}
