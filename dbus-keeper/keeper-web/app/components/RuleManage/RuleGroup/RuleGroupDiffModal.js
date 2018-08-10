import React, {PropTypes, Component} from 'react'
import {Modal, Form, Select, Input, Button, message, Table} from 'antd'
import {FormattedMessage} from 'react-intl'
import OperatingButton from '@/app/components/common/OperatingButton'
import RuleGroupDiffTable from './RuleGroupDiffTable'
// 导入样式
import styles from './res/styles/index.less'
import Request from "@/app/utils/request";

const FormItem = Form.Item
const Option = Select.Option
const TextArea = Input.TextArea

export default class RuleGroupDiffModal extends Component {
  constructor(props) {
    super(props)
    this.state = {}
  }

  render() {
    const {onClose, diffVisible, title, subTitle, content} = this.props
    return (
      <div className={styles.table}>
        <Modal
          visible={diffVisible}
          maskClosable={true}
          width={1000}
          title={'对比'}
          onCancel={onClose}
          footer={[<Button type="primary" onClick={onClose}> 返 回 </Button>]}
        >
          <RuleGroupDiffTable
            title={title}
            subTitle={subTitle}
            content={content}/>
        </Modal>
      </div>
    )
  }
}

RuleGroupDiffModal.propTypes = {}
