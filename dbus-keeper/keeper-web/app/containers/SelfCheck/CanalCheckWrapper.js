import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'
import {createStructuredSelector} from 'reselect'
import {message} from 'antd'
// import Helmet from 'react-helmet'
// 导入自定义组件
import {CanalCheckForm} from '@/app/components/index'
import {makeSelectLocale} from '../LanguageProvider/selectors'
import {CHECK_CANAL_API} from './api'
import Request from '@/app/utils/request'
// 链接reducer和action
@connect(
  createStructuredSelector({
    locale: makeSelectLocale()
  }),
  dispatch => ({})
)
export default class CanalCheckWrapper extends Component {
  constructor (props) {
    super(props)
    this.state = {
      canalStatus: [],
      loading: false
    }
  }

  componentWillMount () {
    this.handleCheckCanal()
  }

  handleCheckCanal = (params) => {
    this.state = {loading: true}
    Request(CHECK_CANAL_API, {
      params: params,
      method: 'get'
    })
      .then(res => {
        if (res && res.status === 0) {
          this.setState({
            canalStatus: res.payload,
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
    const {canalStatus, loading} = this.state
    return (
      <div>
        <CanalCheckForm
          canalStatus={canalStatus}
          loading={loading}
        />
      </div>
    )
  }
}
CanalCheckWrapper.propTypes = {
  locale: PropTypes.any
}
