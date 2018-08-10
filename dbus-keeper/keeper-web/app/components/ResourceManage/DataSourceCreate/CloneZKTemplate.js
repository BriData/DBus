import React, {PropTypes, Component} from 'react'
import {Form, Select, Table, Row, Col, Button, message} from 'antd'
import {FormattedMessage} from 'react-intl'
// 导入样式
import styles from './res/styles/index.less'
import Request, {setToken} from "@/app/utils/request";

import FileTree from './FileTree'
import ZKManageContent from './ZKManageContent'
import OperatingButton from '@/app/components/common/OperatingButton'

const FormItem = Form.Item
const Option = Select.Option

export default class CloneZKTemplate extends Component {
  constructor(props) {
    super(props)
    this.state = {
      currentEditPath: null,
      contentKey: 'contentKey'
    }
  }

  componentWillMount = () => {
    this.handleRefresh()
  }

  handleRefresh = () => {
    const {dataSource} = this.props
    const {loadZkTreeByDsName} = this.props
    loadZkTreeByDsName({dsName: dataSource.dsName, dsType: dataSource.dsType})
  }

  handleClone = () => {
    const {cloneConfFromTemplateApi, dataSource} = this.props
    Request(cloneConfFromTemplateApi, {
      params: {
        dsName: dataSource.dsName,
        dsType: dataSource.dsType
      },
      method: 'get' })
      .then(res => {
        if (res && res.status === 0) {
          this.handleRefresh()
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

  handleRandom = key =>
    `${Math.random()
      .toString(32)
      .substr(3, 8)}${key || ''}`

  handleReadData = path => {
    const {readZkData} = this.props
    readZkData({path})
    this.setState({
      currentEditPath: path,
      contentKey: this.handleRandom('contentKey')
    })
  }

  handleSave = (path, content) => {
    const {saveZkData} = this.props
    saveZkData({path, content})
  }

  handleNext = () => {
    const {onMoveTab} = this.props
    onMoveTab()
  }

  render() {
    const {tree, zkData} = this.props
    const {currentEditPath, contentKey} = this.state
    return (
      <div>
        <Row>
          <Col span={12}>
            <FileTree
              value={tree}
              loadLevelOfPath={this.loadLevelOfPath}
              onReadData={this.handleReadData}
              onClone={this.handleClone}
              onNext={this.handleNext}
            />
          </Col>
          <Col span={12}>
            <ZKManageContent
              key={contentKey}
              path={currentEditPath}
              content={zkData}
              onSave={this.handleSave}
            />
          </Col>
        </Row>
      </div>
    )
  }
}

CloneZKTemplate.propTypes = {}
