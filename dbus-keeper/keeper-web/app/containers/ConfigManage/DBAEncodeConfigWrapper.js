import React, {PropTypes, Component} from 'react'
import {connect} from 'react-redux'
import {message} from 'antd'
import {createStructuredSelector} from 'reselect'
import Helmet from 'react-helmet'
import {makeSelectLocale} from '../LanguageProvider/selectors'
import {
  Bread,
  DBAEncodeConfigSearch,
  DBAEncodeConfigGrid
} from '@/app/components'
import {
  searchDbaEncodeList,
  setDbaEncodeParams
} from './redux'
import Request from "@/app/utils/request";
import {
  DBA_ENCODE_UPDATE_API
} from "./api";
import {
  DBAEncodeConfigModel
} from './selectors'

import {DataSourceModel} from "@/app/containers/ResourceManage/selectors";
import {searchDataSourceIdTypeName} from "@/app/containers/ResourceManage/redux";
// 链接reducer和action
@connect(
  createStructuredSelector({
    locale: makeSelectLocale(),
    DBAEncodeConfigData: DBAEncodeConfigModel(),
    dataSourceData: DataSourceModel(),
  }),
  dispatch => ({
    setDbaEncodeParams: param => dispatch(setDbaEncodeParams(param)),
    searchDbaEncodeList: param => dispatch(searchDbaEncodeList.request(param)),
    searchDataSourceIdTypeName: param => dispatch(searchDataSourceIdTypeName.request(param)),
  })
)
export default class DBAEncodeConfigWrapper extends Component {
  constructor(props) {
    super(props)
    this.initParams = {
      pageNum: 1,
      pageSize: 10
    }
  }

  componentWillMount () {
    // 初始化查询
    const {searchDataSourceIdTypeName} = this.props
    searchDataSourceIdTypeName()
    this.handleSearch(this.initParams, true)
  }

  handleSearch = (params, boolean) => {
    const {searchDbaEncodeList, setDbaEncodeParams} = this.props
    const {DBAEncodeConfigData} = this.props
    const {dbaEncodeParams} = DBAEncodeConfigData
    params = params || dbaEncodeParams
    // 获取用户列表
    for (let key in params) {
      if (!params[key]) delete params[key]
    }
    searchDbaEncodeList(params)
    if (boolean || boolean === undefined) {
      // 存储参数
      setDbaEncodeParams(params)
    }
  };

  /**
   * @param page 跳转页码 type:[object Number]
   * @description 分页查询
   */
  handlePagination = page => {
    const {DBAEncodeConfigData} = this.props
    const {dbaEncodeParams} = DBAEncodeConfigData
    // 分页查询并存储参数
    this.handleSearch({...dbaEncodeParams, pageNum: page}, true)
  };

  /**
   * @param current 当前页码 type:[object Number]
   * @param size 每页需要展示的页数 type:[Object Number]
   * @description 切换页数查询
   */
  handleShowSizeChange = (current, size) => {
    const {DBAEncodeConfigData} = this.props
    const {dbaEncodeParams} = DBAEncodeConfigData
    this.handleSearch(
      {...dbaEncodeParams, pageNum: current, pageSize: size},
      true
    )
  };

  handleDbaEncodeUpdate = record => {
    Request(DBA_ENCODE_UPDATE_API, {data: record, method: 'post'})
      .then(res => {
        if (res && res.status === 0) {
          // 重新查询Table列表
          message.success(res.message)
          this.handleSearch()
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
        path: '/config-manage/dba-encode-config',
        name: 'DBA脱敏配置'
      }
    ]
    const {DBAEncodeConfigData} = this.props
    const {dbaEncodeParams, dbaEncodeList} = DBAEncodeConfigData

    const {dataSourceData} = this.props
    const {dataSourceIdTypeName} = dataSourceData

    console.info(this.props)
    return (
      <div>
        <Helmet
          title="配置中心"
          meta={[
            {name: 'description', content: 'Description of Config Center'}
          ]}
        />
        <Bread source={breadSource}/>
        <DBAEncodeConfigSearch
          dataSourceIdTypeName={dataSourceIdTypeName}
          onSearch={this.handleSearch}
          dbaEncodeParams={dbaEncodeParams}
        />
        <DBAEncodeConfigGrid
          dbaEncodeList={dbaEncodeList}
          onPagination={this.handlePagination}
          onShowSizeChange={this.handleShowSizeChange}
          onDbaEncodeUpdate={this.handleDbaEncodeUpdate}
        />
      </div>
    )
  }
}
DBAEncodeConfigWrapper.propTypes = {
  locale: PropTypes.any
}
