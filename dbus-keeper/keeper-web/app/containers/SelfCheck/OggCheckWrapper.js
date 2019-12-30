import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'
import {createStructuredSelector} from 'reselect'
import {message} from 'antd'
// import Helmet from 'react-helmet'
// 导入自定义组件
import {OggCheckForm} from '@/app/components/index'
import {makeSelectLocale} from '../LanguageProvider/selectors'
import {CHECK_OGG_API} from './api'
import Request from '@/app/utils/request'
// 链接reducer和action
@connect(
  createStructuredSelector({
    locale: makeSelectLocale()
  }),
  dispatch => ({})
)
export default class OggCheckWrapper extends Component {
  constructor (props) {
    super(props)
    this.state = {
      oggStatus: [],
      loading: false
    }
  }

  componentWillMount () {
    this.handleCheckOgg()
  }

  handleCheckOgg = (params) => {
    this.setState({loading: true})
    Request(CHECK_OGG_API, {
      params: params,
      method: 'get'
    })
      .then(res => {
        if (res && res.status === 0) {
          this.setState({
            oggStatus: res.payload,
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
    const {oggStatus, loading} = this.state
    return (
      <div>
        <OggCheckForm
          oggStatus={oggStatus}
          loading={loading}
        />
      </div>
    )
  }
}
OggCheckWrapper.propTypes = {
  locale: PropTypes.any
}
