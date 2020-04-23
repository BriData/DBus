import React, { PropTypes, Component } from 'react'
import { Steps, Modal, Form, Select, Input, Button, message,Table } from 'antd'
const Step = Steps.Step
import { FormattedMessage } from 'react-intl'
import OperatingButton from '@/app/components/common/OperatingButton'

// 导入样式
import styles from './res/styles/index.less'
import Request from "@/app/utils/request";

const FormItem = Form.Item
const Option = Select.Option
const TextArea = Input.TextArea


export default class DataSourceManageCheckModal extends Component {
  constructor (props) {
    super(props)
    this.state = {
    }
  }

  /**
   * @param render 传入一个render
   * @returns render 返回一个新的render
   * @description 统一处理render函数
   */
  renderComponent = render => (text, record, index) =>
    render(text, record, index);

  /**
   * @description 默认的render
   */
  renderNomal = (text, record, index) => (
    <div title={text} className={styles.ellipsis}>
      {text}
    </div>
  )

  render () {
    const {key, visible, result, loading, onClose} = this.props
    const {firstStep, secondStep, thirdStep, fourthStep} = result

    let status = 'process'
    let current = 0
    if (loading) {
      status = 'process'
      current = 0
    } else if (!firstStep) {
      status = 'error'
      current = 1
    } else if (!secondStep) {
      status = 'error'
      current = 2
    } else if (!thirdStep) {
      status = 'error'
      current = 3
    } else if (!fourthStep) {
      status = 'error'
      current = 4
    } else {
      status = 'finish'
      current = 5
    }
    return (
      <div className={styles.table}>
        <Modal
          closable={false}
          key={key}
          visible={visible}
          maskClosable={false}
          width={1000}
          title={<FormattedMessage
            id="app.components.resourceManage.dataTable.checkDataLine"
            defaultMessage="检查数据线"
          />}
          onCancel={onClose}
          footer={[<Button loading={loading} type="primary" onClick={onClose}> 返 回 </Button>]}
        >
          <Steps current={current} status={status}>
            <Step title={<FormattedMessage
              id="app.components.resourceManage.dataTable.checkStart"
              defaultMessage="检查开始"
            />} />
            <Step title={<FormattedMessage
              id="app.components.resourceManage.dataTable.insertHeartbeat"
              defaultMessage="插入心跳"
            />} />
            <Step title={<FormattedMessage
              id="app.components.resourceManage.dataTable.canalOgg"
              defaultMessage="Canal OGG 进程"
            />} />
            <Step title={<FormattedMessage
              id="app.components.resourceManage.dataTable.dispatcherProcess"
              defaultMessage="分发进程"
            />} />
            <Step title={<FormattedMessage
              id="app.components.resourceManage.dataTable.appenderProcess"
              defaultMessage="增量进程"
            />} />
            <Step title={<FormattedMessage
              id="app.components.resourceManage.dataTable.checkEnd"
              defaultMessage="检查结束"
            />} />
          </Steps>
        </Modal>
      </div>
    )
  }
}

DataSourceManageCheckModal.propTypes = {
}
