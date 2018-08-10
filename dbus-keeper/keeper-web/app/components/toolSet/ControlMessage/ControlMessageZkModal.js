import React, { PropTypes, Component } from 'react'
import { Modal, Form, Select, Input, Button, message,Table } from 'antd'
import { FormattedMessage } from 'react-intl'
import OperatingButton from '@/app/components/common/OperatingButton'

// 导入样式
import styles from './res/styles/index.less'
import Request from "@/app/utils/request";

const FormItem = Form.Item
const Option = Select.Option
const TextArea = Input.TextArea

@Form.create()
export default class ControlMessageZkModal extends Component {
  constructor (props) {
    super(props)
  }

  render () {
    const {key, visible, onClose, zkData} = this.props
    return (
      <div>
        <Modal
          key={key}
          visible={visible}
          maskClosable={true}
          width={1000}
          title={'ZK内容'}
          onCancel={onClose}
          onOk={onClose}
        >
          <TextArea value={zkData} wrap='off' autosize={{minRows:10, maxRows: 24}}/>
        </Modal>
      </div>
    )
  }
}

ControlMessageZkModal.propTypes = {
}
