import React, {PropTypes, Component} from 'react'
import {Modal, Form, Select,Popconfirm, Input, Button, message, Table, Row, Col} from 'antd'
import {FormattedMessage} from 'react-intl'
import OperatingButton from '@/app/components/common/OperatingButton'

import FileTree from './FileTree'
import ZKManageContent from './ZKManageContent'
// 导入样式
import styles from '../res/styles/index.less'
import Request from "@/app/utils/request";

const FormItem = Form.Item
const Option = Select.Option
const TextArea = Input.TextArea


@Form.create()
export default class DataSourceManageZkConfigModal extends Component {
  constructor(props) {
    super(props)
    this.state = {
      contentKey: 'contentKey',
      currentEditPath: null
    }
  }


  componentWillMount = () => {

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

  render() {
    const {tree, key, visible, onClose, onReset, zkData} = this.props
    const {currentEditPath, contentKey} = this.state
    return (
      <Modal
        className="top-modal"
        key={key}
        visible={visible}
        maskClosable={false}
        width={1000}
        title={<FormattedMessage
          id="app.components.resourceManage.dataSource.dsZkConfig"
          defaultMessage="数据源ZK配置"
        />}
        onCancel={onClose}
        footer={[
          <Button type="primary" onClick={onClose}>
            <FormattedMessage
              id="app.common.back"
              defaultMessage="返回"
            />
          </Button>]}
      >
        <Row>
          <Col span={12}>
            <FileTree
              value={tree}
              loadLevelOfPath={() => {}}
              onReadData={this.handleReadData}
              onAdd={() => {}}
              onDelete={() => {}}
            />
            <Popconfirm title={<FormattedMessage
              id="app.components.resourceManage.dataSource.confirmRestoreDefault"
              defaultMessage="确定恢复初始化配置？"
            />} onConfirm={onReset} okText="Yes" cancelText="No">
              <Button style={{marginTop: 10}} type="danger">
                <FormattedMessage
                  id="app.components.resourceManage.dataSource.restoreDefault"
                  defaultMessage="恢复初始化配置"
                />
              </Button>
            </Popconfirm>
          </Col>
          <Col span={12}>
            <ZKManageContent
              key={contentKey}
              path={currentEditPath}
              zkData={zkData}
              onSave={this.handleSave}
            />
          </Col>
        </Row>
      </Modal>
    )
  }
}

DataSourceManageZkConfigModal.propTypes = {}
