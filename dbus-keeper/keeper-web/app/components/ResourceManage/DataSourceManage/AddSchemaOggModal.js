import React, {PropTypes, Component} from 'react'
import {Row, Col, Modal, Form, Select, Input, Spin, Table, Icon, message} from 'antd'
import {FormattedMessage} from 'react-intl'
import OperatingButton from '@/app/components/common/OperatingButton'

// 导入样式
import styles from './res/styles/index.less'

const FormItem = Form.Item
const Option = Select.Option
const TextArea = Input.TextArea

@Form.create()
export default class AddSchemaOggModal extends Component {
  constructor(props) {
    super(props)
  }

  render() {
    const {visible, key, content, onClose} = this.props
    return (
      <Modal
        className="top-modal"
        visible={visible}
        maskClosable={false}
        key={key}
        onCancel={onClose}
        onOk={onClose}
        width={1000}
        title={<FormattedMessage id="app.components.resourceManage.dataSource.oggScript" defaultMessage="OGG脚本" />}
      >
        <TextArea value={content} wrap="off" autosize={{minRows:10}}/>
      </Modal>
    )
  }
}

AddSchemaOggModal.propTypes = {}
