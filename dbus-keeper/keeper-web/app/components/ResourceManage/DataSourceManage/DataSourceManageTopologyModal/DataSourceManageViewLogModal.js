import React, { PropTypes, Component } from 'react'
import { Modal, Form, Spin, Select, Input, Button, message,Table } from 'antd'
import { FormattedMessage } from 'react-intl'
import OperatingButton from '@/app/components/common/OperatingButton'

// 导入样式
import styles from '../res/styles/index.less'
import Request from "@/app/utils/request";

const FormItem = Form.Item
const Option = Select.Option
const TextArea = Input.TextArea

@Form.create()
export default class DataSourceManageViewLogModal extends Component {
  constructor (props) {
    super(props)
    this.state = {
    }
    this.textarea = null
  }

  componentDidUpdate = () => {
    setTimeout(this.textareaAutoScroll, 0)
  }

  textareaAutoScroll = () => {
    if (this.textarea) {
      this.textarea.textAreaRef.scrollTop = this.textarea.textAreaRef.scrollHeight
    }
  }

  render () {
    const {key, visible, record, onClose, onRefresh,loading, content} = this.props
    const formItemLayout = {
      labelCol: {
        xs: { span: 2 },
        sm: { span: 2 }
      },
      wrapperCol: {
        xs: { span: 20 },
        sm: { span: 20 }
      }
    }
    const tailItemLayout = {
      wrapperCol: {
        xs: { offset: 2, span: 20 },
        sm: { offset: 2, span: 20 }
      }
    }
    return (
      <div className={styles.table}>
        <Modal
          className="top-modal"
          key={key}
          visible={visible}
          maskClosable={false}
          width={1000}
          title={content.runningInfo}
          onCancel={onClose}
          footer={[<Button type="primary" onClick={onClose}>
            <FormattedMessage
              id="app.common.back"
              defaultMessage="返回"
            />
          </Button>]}
        >
          <Form>
            <FormItem label={<FormattedMessage
              id="app.common.log"
              defaultMessage="日志"
            />} {...formItemLayout}>
              <Spin spinning={loading} tip="正在加载数据中...">
                <TextArea ref={ref => (this.textarea = ref)} wrap='off' value={content.execResult} autosize={{minRows: 10, maxRows: 24}}/>
              </Spin>

            </FormItem>
            <FormItem {...tailItemLayout}>
              <Button loading={loading} onClick={() => onRefresh(record)}>
                <FormattedMessage
                  id="app.common.refresh"
                  defaultMessage="刷新"
                />
              </Button>
            </FormItem>
          </Form>
        </Modal>
      </div>
    )
  }
}

DataSourceManageViewLogModal.propTypes = {
}
