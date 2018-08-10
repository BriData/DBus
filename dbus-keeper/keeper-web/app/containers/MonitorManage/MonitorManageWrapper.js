import React, {PropTypes, Component} from 'react'
import {connect} from 'react-redux'
import {createStructuredSelector} from 'reselect'
import Helmet from 'react-helmet'
import {message} from 'antd'
// 导入自定义组件
import {READ_ZK_PROPERTIES_API} from '@/app/containers/ConfigManage/api'
// selectors
import {makeSelectLocale} from '../LanguageProvider/selectors'
import Request from "@/app/utils/request";

const ZK_PATH = '/DBus/Commons/global.properties'

// 链接reducer和action
@connect(
  createStructuredSelector({
    locale: makeSelectLocale()
  }),
  dispatch => ({})
)
export default class MonitorManageWrapper extends Component {
  constructor(props) {
    super(props)
    this.state = {
      url: 'about:blank'
    }
  }

  componentWillMount = () => {
    window.onresize = () => {
      this.resetFrame()
    }

    Request(READ_ZK_PROPERTIES_API, {
      params: {
        path: ZK_PATH
      },
      method: 'get'
    })
      .then(res => {
        if (res && res.status === 0) {
          this.setState({url: res.payload.monitor_url})
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

  resetFrame = () => {
    const iframe = document.getElementsByTagName('iframe')[0]
    iframe.width = `${document.body.offsetWidth}px`
    iframe.height = `${document.body.offsetHeight - 50}px`
  }

  render() {
    return (
      <iframe
        src={this.state.url}
        style={{margin: -12}}
        frameBorder='0'
        width={document.body.offsetWidth}
        height={document.body.offsetHeight - 50}
      />
    )
  }
}
MonitorManageWrapper.propTypes = {
  locale: PropTypes.any,
  searchSinkList: PropTypes.func,
  setSearchSinkParam: PropTypes.func
}
