import React, {PropTypes, Component} from 'react'
import {connect} from 'react-redux'
import {message} from 'antd'
import md5 from 'js-md5'
import {createStructuredSelector} from 'reselect'
import Helmet from 'react-helmet'
import {makeSelectLocale} from '../LanguageProvider/selectors'
import {
  Bread,
  DBusMgrConfigForm
} from '@/app/components'
import Request from "@/app/utils/request";
import {
  READ_ZK_DATA_API,
  UPDATE_MGR_DB_API,
  RESET_MGR_DB_API
} from "./api";

import {LOGIN_API} from "@/app/containers/Login/api";

const ZK_PATH = '/DBus/Commons/mysql.properties'

// 链接reducer和action
@connect(
  createStructuredSelector({
    locale: makeSelectLocale(),
  }),
  dispatch => ({
  })
)
export default class DBusMgrConfigWrapper extends Component {
  constructor(props) {
    super(props)
    this.state = {
      zkData: null,
      isLogin: false
    }
  }

  fetchZkdata() {
    Request(READ_ZK_DATA_API, {
      params: {
        path: ZK_PATH,
        admin: 'admin'
      }
    })
      .then(res => {
        if (res && res.status === 0) {
          this.setState({zkData: res.payload.content})
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

  handleReset = data => {
    this.setState({
      zkData: data
    })

    Request(RESET_MGR_DB_API, {
      data: {
        content: data
      },
      method: 'post'
    })
      .then(res => {
        if (res && res.status === 0) {
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

  handleSave = data => {
    this.setState({
      zkData: data
    })

    Request(UPDATE_MGR_DB_API, {
      data: {
        content: data
      },
      method: 'post'
    })
      .then(res => {
        if (res && res.status === 0) {
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

  handleLogin = params => {
    Request(LOGIN_API, {
      params: {
        encoded: true
      },
      data: {
        ...params,
        password: md5(params.password)
      },
      method: 'post'
    })
      .then(res => {
        if (res && res.status === 0) {
          this.setState({isLogin: true})
          this.fetchZkdata()
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
        path: '/config-manage/dbus-mgr-config',
        name: '管理库配置'
      }
    ]
    console.info(this.props)
    const {zkData,isLogin} = this.state
    return (
      <div>
        <Helmet
          title="数据源管理"
          meta={[
            {name: 'description', content: 'Description of DataSource Manage'}
          ]}
        />
        <Bread source={breadSource}/>
        <DBusMgrConfigForm
          data={zkData}
          isLogin={isLogin}
          onSave={this.handleSave}
          onReset={this.handleReset}
          onLogin={this.handleLogin}
        />
      </div>
    )
  }
}
DBusMgrConfigWrapper.propTypes = {
  locale: PropTypes.any
}
