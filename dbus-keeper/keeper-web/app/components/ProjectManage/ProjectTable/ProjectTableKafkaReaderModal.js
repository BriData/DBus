import React, { PropTypes, Component } from 'react'
import {Button, Modal, Form, Select, Input } from 'antd'
import { FormattedMessage } from 'react-intl'
import {
  KafkaReaderForm
} from '@/app/components'
// 导入样式
import styles from './res/styles/index.less'

const FormItem = Form.Item
const Option = Select.Option
const TextArea = Input.TextArea

@Form.create()
export default class ProjectTableKafkaReaderModal extends Component {
  constructor (props) {
    super(props)
    this.state = {
    }
  }

  componentWillMount() {
    const {getTopicsByUserId} = this.props
    getTopicsByUserId()
  }

  handleRead = values => {
    const {readKafkaData} = this.props
    readKafkaData(values)
  }

  render () {
    const {key, visible, record, onClose} = this.props
    const topicList = (this.props.topicsByUserIdList.result.payload || []).sort()
    const kafkaData = this.props.kafkaData
    return (
      <div>
        <Modal
          className="ant-modal-max"
          visible={visible}
          maskClosable={true}
          title={<FormattedMessage
            id="app.components.projectManage.projectTable.readKafkaTopic"
            defaultMessage="读取Kafka Topic"
          />}
          onCancel={onClose}
          footer={[<Button type="primary" onClick={onClose}> 返 回 </Button>]}
        >
          <KafkaReaderForm
            topicList={topicList}
            kafkaData={kafkaData}
            onRead={this.handleRead}
            record={record}
          />
        </Modal>
      </div>
    )
  }
}

ProjectTableKafkaReaderModal.propTypes = {
}
