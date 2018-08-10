import React, { PropTypes, Component } from 'react'
import { connect } from 'react-redux'
import { createStructuredSelector } from 'reselect'
import Request, {setToken} from '@/app/utils/request'
import Helmet from 'react-helmet'
import {message} from 'antd'
// 导入自定义组件
import {
  Bread,
  DBusDataManageSearch,
  DBusDataManageGrid,
  DBusDataManageQueryModal
} from '@/app/components'
import { makeSelectLocale } from '../LanguageProvider/selectors'
import {DBusDataModel, DataSourceModel} from './selectors'

import {
  clearSource,
  searchFromSource,
  searchDataSourceIdTypeName
} from './redux'

import {
  DBUS_DATA_EXECUTE_SQL_API
} from './api'


// 链接reducer和action
@connect(
  createStructuredSelector({
    DBusDataData: DBusDataModel(),
    DataSourceData: DataSourceModel()
  }),
  dispatch => ({
    searchDataSourceIdTypeName: param => dispatch(searchDataSourceIdTypeName.request(param)),
    searchFromSource: param => dispatch(searchFromSource.request(param)),
    clearSource: param => dispatch(clearSource(param))
  })
)
export default class DBusDataWrapper extends Component {
  constructor (props) {
    super(props)
    this.state = {
      visible: false,
      key: 'key',
      sql: null,
      dsId: null,
      loading: false,
      executeResult: [],
    }
  }
  componentWillMount() {
    const {searchDataSourceIdTypeName} = this.props
    searchDataSourceIdTypeName()
    const {location} = this.props
    const {query} = location
    const {dsId} = query
    if (dsId) {
      this.handleDataSourceChange(`${dsId}`)
    }
  }

  componentWillUnmount = () => {
    const {clearSource} = this.props
    clearSource()
  }

  handleRandom = key =>
    `${Math.random()
      .toString(32)
      .substr(3, 8)}${key || ''}`

  handleDataSourceChange = value => {
    if (!value) return
    const {searchFromSource} = this.props
    searchFromSource({
      dsId: value,
    })
    this.setState({dsId: value})
  }

  handleOpenAccessSourceModal = record => {
    const columns = record.column.split('\n')
    let sql = "".concat(...columns.map((column, index) => {
      if (!column) return ''
      const field = column.split(',')[0]
      if (index) return ` ,${field} `
      else return ` ${field} `
    }))
    sql = `select ${sql} from ${record.name}`
    this.setState({
      visible: true,
      key: this.handleRandom('key'),
      sql: sql
    })
  }

  handleCloseAccessSourceModal = () => {
    this.setState({
      visible: false,
      key: this.handleRandom('key')
    })
  }

  handleExecuteSql = values => {
    this.setState({loading: true})
    Request(DBUS_DATA_EXECUTE_SQL_API, {
      data: {
        ...values,
        dsId: this.state.dsId
      },
      method: 'post'
    })
      .then(res => {
        if (res && res.status === 0) {
          this.setState({executeResult: res.payload})
        } else {
          message.warn(res.message)
        }
        this.setState({loading: false})
      })
      .catch(error => {
        error.response.data && error.response.data.message
          ? message.error(error.response.data.message)
          : message.error(error.message)
        this.setState({loading: false})
      })
  }

  render () {
    console.info(this.props)
    const breadSource = [
      {
        path: '/resource-manage',
        name: 'home'
      },
      {
        path: '/resource-manage',
        name: '数据源管理'
      },
      {
        path: '/resource-manage/dbus-data',
        name: 'DBusData管理'
      }
    ]
    const dataSourceList = Object.values(this.props.DataSourceData.dataSourceIdTypeName.result)
    const sourceList = Object.values(this.props.DBusDataData.sourceList.result).map(str => JSON.parse(str))
    const {visible, key, sql, loading,dsId, executeResult} = this.state
    return (
      <div>
        <Helmet
          title="数据源管理"
          meta={[
            { name: 'description', content: 'Description of DataSource Manage' }
          ]}
        />
        <Bread source={breadSource} />
        <DBusDataManageSearch
          dataSourceList={dataSourceList}
          onDataSourceChange={this.handleDataSourceChange}
          dsId={dsId}
        />
        <DBusDataManageGrid
          sourceList={sourceList}
          onAccessSource={this.handleOpenAccessSourceModal}
        />
        <DBusDataManageQueryModal
          visible={visible}
          key={key}
          sql={sql}
          executeResult={executeResult}
          onExecuteSql={this.handleExecuteSql}
          onClose={this.handleCloseAccessSourceModal}
          loading={loading}
        />
      </div>
    )
  }
}
DBusDataWrapper.propTypes = {
  locale: PropTypes.any,
}
