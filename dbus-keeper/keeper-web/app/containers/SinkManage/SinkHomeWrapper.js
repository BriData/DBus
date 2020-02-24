import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'
import {message} from 'antd'
import {createStructuredSelector} from 'reselect'
import Helmet from 'react-helmet'
// 导入自定义组件
import {Bread, DataSourceManageMountModal, SinkForm, SinkManageGrid, SinkManageSearch} from '@/app/components'
// selectors
import {sinkHomeModel} from './selectors'
import {makeSelectLocale} from '../LanguageProvider/selectors'
// action
import {searchSinkList, setSearchSinkParam} from './redux'
import {intlMessage} from "@/app/i18n";
import Request from "@/app/utils/request";
import {GET_MOUNT_PROJECT_API} from "@/app/containers/ProjectManage/api";

// 链接reducer和action
@connect(
  createStructuredSelector({
    sinkHomeData: sinkHomeModel(),
    locale: makeSelectLocale()
  }),
  dispatch => ({
    searchSinkList: param => dispatch(searchSinkList.request(param)),
    setSearchSinkParam: param => dispatch(setSearchSinkParam(param))
  })
)
export default class SinkHomeWrapper extends Component {
  constructor(props) {
    super(props)
    this.state = {
      modalKey: '',
      visible: false,
      modalStatus: 'create',
      sinkInfo: null,

      mountModalKey: 'mountModalKey',
      mountModalVisible: false,
      mountModalContent: []
    }
    this.tableWidth = [
      '10%',
      '30%',
      '10%',
      '30%',
      '240px'
    ]
    this.initParams = {
      pageNum: 1,
      pageSize: 10
    }
  }

  handleModalVisible = (visible, modalStatus = 'create', sinkInfo = null) => {
    this.setState({visible, modalStatus, sinkInfo})
    if (visible === false) this.setState({modalKey: this.handleRandom('sink')})
  };

  /**
   * @param key 传入一个key type:[Object String]  默认:空
   * @returns String 返回一个随机字符串
   */
  handleRandom = key =>
    `${Math.random()
      .toString(32)
      .substr(3, 8)}${key || ''}`;

  componentWillMount() {
    // 初始化查询
    this.handleSearch(this.initParams)
  }

  /**
   * @param params 查询的参数 type:[Object Object]
   * @description 查询Sink列表
   */
  handleSearch = (params) => {
    const {searchSinkList, setSearchSinkParam} = this.props
    searchSinkList(params)
    setSearchSinkParam(params)
  };
  /**
   * @param page  传入的跳转页码  type:[Object Number]
   * @description sink分页
   */
  handlePagination = page => {
    const {sinkHomeData} = this.props
    const {sinkParams} = sinkHomeData
    this.handleSearch({...sinkParams, pageNum: page})
  };

  /**
   * @description 获取到sink信息并弹窗
   */
  handleGetSinkInfo = (sinkInfo) => {
    this.handleModalVisible(true, 'modify', sinkInfo)
  }

  handleMount = record => {
    Request(GET_MOUNT_PROJECT_API, {
      params: {
        sinkId: record.id
      },
      method: 'get'
    })
      .then(res => {
        if (res && res.status === 0) {
          this.handleOpenMountModal(res.payload)
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

  handleOpenMountModal = content => {
    this.setState({
      mountModalKey: this.handleRandom('mountModalKey'),
      mountModalVisible: true,
      mountModalContent: content
    })
  }

  handleCloseMountModal = () => {
    this.setState({
      mountModalKey: this.handleRandom('mountModalKey'),
      mountModalVisible: false
    })
  }

  render() {
    const {modalKey, visible, modalStatus, sinkInfo} = this.state
    const {
      locale,
      sinkHomeData
    } = this.props
    const {
      sinkParams,
      sinkList
    } = sinkHomeData
    const localeMessage = intlMessage(locale)
    const breadSource = [
      {
        path: '/sink-manage',
        name: 'home'
      },
      {
        path: '/sink-manage',
        name: 'Sink管理'
      }
    ]
    const {mountModalContent, mountModalVisible, mountModalKey} = this.state
    return (
      <div>
        <Helmet
          title="Sink"
          meta={[{name: 'description', content: 'Sink Manage'}]}
        />
        <Bread source={breadSource}/>
        <SinkManageSearch
          locale={locale}
          sinkParams={sinkParams}
          onShowModal={this.handleModalVisible}
          onSearch={this.handleSearch}
        />
        <SinkManageGrid
          locale={locale}
          tableWidth={this.tableWidth}
          sinkParams={sinkParams}
          sinkList={sinkList.result.payload}
          onModify={this.handleGetSinkInfo}
          onSearch={this.handleSearch}
          onPagination={this.handlePagination}
          onMount={this.handleMount}
        />
        <SinkForm
          modalKey={modalKey}
          locale={locale}
          modalStatus={modalStatus}
          visible={visible}
          sinkInfo={sinkInfo}
          sinkParams={sinkParams}
          onSearch={this.handleSearch}
          onCloseModal={this.handleModalVisible}
        />
        <DataSourceManageMountModal
          key={mountModalKey}
          visible={mountModalVisible}
          content={mountModalContent}
          onClose={this.handleCloseMountModal}
        />
      </div>
    )
  }
}
SinkHomeWrapper.propTypes = {
  locale: PropTypes.any,
  searchSinkList: PropTypes.func,
  setSearchSinkParam: PropTypes.func
}
