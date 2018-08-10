import React, { PropTypes, Component } from 'react'
import { Modal, Form, Select, Input, Button, message,Table } from 'antd'
import { FormattedMessage } from 'react-intl'
import OperatingButton from '@/app/components/common/OperatingButton'
import JSONTree from 'react-json-tree'
// 导入样式
import styles from './res/styles/index.less'
import Request from "@/app/utils/request";

const FormItem = Form.Item
const Option = Select.Option
const TextArea = Input.TextArea

@Form.create()
export default class RuleGroupCloneModal extends Component {
  constructor (props) {
    super(props)
    this.state = {
    }
  }

  render () {
    const { getFieldDecorator } = this.props.form
    const {key, visible,ums, onClose} = this.props
    const theme = {
      scheme: 'monokai',
      base00: '#272822'
    }
    return (
      <div className={styles.table}>
        <Modal
          key={key}
          visible={visible}
          maskClosable={true}
          width={1000}
          title={'UMS'}
          onCancel={onClose}
          footer={[<Button type="primary" onClick={onClose}> 返 回 </Button>]}
        >
          <JSONTree data={ums} theme={theme} />
        </Modal>
      </div>
    )
  }
}

RuleGroupCloneModal.propTypes = {
}
