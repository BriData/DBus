import React, {PropTypes, Component} from 'react'
import {connect} from 'react-redux'
import {createStructuredSelector} from 'reselect'
import {message} from 'antd'
// import Helmet from 'react-helmet'
// 导入自定义组件
import {
  ControlMessageForm,
  ControlMessageZkModal
} from '@/app/components'
import {makeSelectLocale} from '../LanguageProvider/selectors'
import {ControlMessageModel} from './selectors'
import {ProjectTableModel} from '../ProjectManage/selectors'
import {
  searchDataSourceList,
  sendControlMessage,
  readReloadInfo,
  getTopologyList
} from '@/app/containers/toolSet/redux'
// 链接reducer和action
@connect(
  createStructuredSelector({
    ControlMessageData: ControlMessageModel(),
    TopologyTableData: ProjectTableModel(),
    locale: makeSelectLocale()
  }),
  dispatch => ({
    searchDataSourceList: param => dispatch(searchDataSourceList.request(param)),
    sendControlMessage: param => dispatch(sendControlMessage.request(param)),
    readReloadInfo: param => dispatch(readReloadInfo.request(param)),
    getTopologyList: param => dispatch(getTopologyList.request(param)),
  })
)
export default class ControlMessageWrapper extends Component {
  constructor(props) {
    super(props)
    this.state = {
      zkModalKey: 'zkModalKey',
      zkModalVisible: false,
    }
  }

  componentWillMount() {
    const {searchDataSourceList, getTopologyList} = this.props
    searchDataSourceList()
    getTopologyList()
  }

  handleSend = data => {
    const {sendControlMessage} = this.props
    sendControlMessage(data)
  }

  handleRandom = key =>
    `${Math.random()
      .toString(32)
      .substr(3, 8)}${key || ''}`

  handleOpenZkModal = messageType => {
    if (!messageType) {
      message.error('请选择消息类型')
      return
    }
    this.setState({
      zkModalVisible: true
    })
    const {readReloadInfo} = this.props
    readReloadInfo({type: messageType})
  }

  handleCloseZkModal = () => {
    this.setState({
      zkModalKey: this.handleRandom('zkModalKey'),
      zkModalVisible: false
    })
  }

  render() {
    console.info(this.props)
    const {ControlMessageData, TopologyTableData} = this.props
    const {dataSourceList} = ControlMessageData
    const {topologyList} = TopologyTableData
    const {zkModalKey, zkModalVisible} = this.state
    const zkData = JSON.stringify(ControlMessageData.reloadInfo.result.payload || {}, null, '\t')
    return (
      <div>
        <ControlMessageForm
          dataSourceList={dataSourceList && dataSourceList.result && dataSourceList.result.payload}
          topologyList={topologyList && topologyList.result}
          onSend={this.handleSend}
          onReadZk={this.handleOpenZkModal}
        />
        <ControlMessageZkModal
          key={zkModalKey}
          visible={zkModalVisible}
          onClose={this.handleCloseZkModal}
          zkData={zkData}
        />
      </div>
    )
  }
}
ControlMessageWrapper.propTypes = {
  locale: PropTypes.any
}
