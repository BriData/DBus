import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'
import {createStructuredSelector} from 'reselect'
import Helmet from 'react-helmet'
import Request from '@/app/utils/request'
import {message} from 'antd'
// 导入自定义组件
import {Bread, JarManageGrid, JarManageModifyModal, JarManageSearch, JarManageUploadModal} from '@/app/components'
// selectors
import {JarManageModel} from './selectors'
import {makeSelectLocale} from '../LanguageProvider/selectors'
// action
import {searchJarInfos} from './redux'

import {BATCH_DELETE_JAR_API, DELETE_JAR_API, JAR_UPDATE_API, UPLOAD_JAR_API} from './api'

// 链接reducer和action
@connect(
  createStructuredSelector({
    JarManageData: JarManageModel(),
    locale: makeSelectLocale()
  }),
  dispatch => ({
    searchJarInfos: param => dispatch(searchJarInfos.request(param))
  })
)
export default class JarManageWrapper extends Component {
  constructor(props) {
    super(props)
    this.tableWidth = [
      '10%',
      '5%',
      '6%',
      '6%',
      '10%',
      '15%',
      '8%',
      '35%',
      '10%'
    ]
    this.selectedRows = []
    this.state = {
      category: 'normal',
      version: '',
      type: '',
      modalKey: '',
      visible: false,
      uploadVersion: null,
      uploadType: null,
      modifyModalKey: 'modifyModalKey',
      modifyModalVisible: false,
      modifyModalRecord: {},
    }
  }

  componentWillMount() {
    // 初始化查询
    this.handleSearch({
      category: 'normal',
      version: '',
      type: '',
    })
  }

  /**
   * @description 查询Jar列表
   */
  handleSearch = (filterParams) => {
    const {searchJarInfos} = this.props
    searchJarInfos({
      category: filterParams.category,
      version: filterParams.version,
      type: filterParams.type,
    })
    this.setState({
      ...filterParams
    })
  }

  handleSearchParamChange = values => {
    this.setState({
      ...values
    })
  }

  handleSearchParamReset = () => {
    this.setState({
      category: 'normal',
      version: '',
      type: ''
    })
  }

  handleSelectChange = (selectedRowKeys, selectedRows) => {
    this.selectedRows = selectedRows
  }

  handleBatchDelete = () => {
    Request(BATCH_DELETE_JAR_API, {
      data: this.selectedRows.map(row => row.id),
      method: 'post'
    })
      .then(res => {
        if (res && res.status === 0) {
          // 重新查询项目列表
          this.handleSearch(this.state)
        } else {
          message.warn(res.message)
        }
      })
      .catch(error => {
        error.response.data && error.response.data.message
          ? message.error(error.response.data.message)
          : message.error(error.message)
      })
    this.selectedRows = []
  }

  handleDelete = (id) => {
    Request(`${DELETE_JAR_API}/${id}`, {
      method: 'get'
    })
      .then(res => {
        if (res && res.status === 0) {
          // 重新查询项目列表
          this.handleSearch(this.state)
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

  handleUpdate = (values) => {
    Request(JAR_UPDATE_API, {
      data: {...values},
      method: 'post'
    })
      .then(res => {
        if (res && res.status === 0) {
          this.handleCloseModify()
          // 重新查询项目列表
          this.handleSearch(this.state)
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

  handleUploadJarModal = (visible) => {
    this.setState({visible})
    if (!visible) {
      this.handleSearch({
        ...this.state,
        visible,
        uploadVersion: null,
        uploadType: null,
        modalKey: this.handleRandom('modal')
      })
    }
    if (visible) {
      this.setState({modalKey: this.handleRandom('modal')})
    }
  }

  handleChangeUploadParam = (param) => {
    this.setState({...param})
  }

  handleOpenModifyModal = record => {
    this.setState({
      modifyModalVisible: true,
      modifyModalRecord: record
    })
  }

  handleCloseModify = () => {
    this.setState({
      modifyModalKey: this.handleRandom('modify'),
      modifyModalVisible: false
    })
  }

  /**
   * @param key 传入一个key type:[Object String]  默认:空
   * @returns 返回一个随机字符串
   */
  handleRandom = key =>
    `${Math.random()
      .toString(32)
      .substr(3, 8)}${key || ''}`;

  render() {
    const {visible, uploadVersion, uploadType, modalKey} = this.state
    const {modifyModalKey, modifyModalVisible, modifyModalRecord} = this.state
    const {locale, JarManageData} = this.props
    const {jarInfos} = JarManageData
    const {category} = this.state
    const breadSource = [
      {
        path: '/project/home',
        name: 'home'
      },
      {
        path: '/resource-manage/data-source',
        name: '数据源管理'
      },
      {
        path: '/resource-manage/jar-manager',
        name: 'Jar管理'
      }
    ]
    return (
      <div>
        <Helmet
          title="数据源管理"
          meta={[
            {name: 'description', content: 'Description of DataSource Manage'}
          ]}
        />
        <Bread source={breadSource}/>
        <JarManageSearch
          onBatchDelete={this.handleBatchDelete}
          onUploadJar={() => this.handleUploadJarModal(true)}
          onSearch={this.handleSearch}
          onSearchParamChange={this.handleSearchParamChange}
          filterParams={this.state}
          onReset={this.handleSearchParamReset}
        />
        <JarManageGrid
          visible={modifyModalVisible}
          tableWidth={this.tableWidth}
          jarInfos={jarInfos}
          onSelectChange={this.handleSelectChange}
          onDelete={this.handleDelete}
          onModify={this.handleOpenModifyModal}
        />
        <JarManageUploadModal
          key={modalKey}
          visible={visible}
          onModalCancel={() => this.handleUploadJarModal(false)}
          uploadVersion={uploadVersion}
          uploadType={uploadType}
          category={category}
          onChangeUploadParam={this.handleChangeUploadParam}
          api={UPLOAD_JAR_API}
        />
        <JarManageModifyModal
          key={modifyModalKey}
          visible={modifyModalVisible}
          jarInfo={modifyModalRecord}
          onUpdate={this.handleUpdate}
          onClose={this.handleCloseModify}
        />
      </div>
    )
  }
}
JarManageWrapper.propTypes = {
  locale: PropTypes.any
}
