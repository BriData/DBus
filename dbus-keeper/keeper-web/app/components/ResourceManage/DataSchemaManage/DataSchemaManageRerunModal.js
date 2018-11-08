import React, { PropTypes, Component } from 'react'
import { Modal, Form, Select, Input, Button, message,Table, Spin } from 'antd'
import { FormattedMessage } from 'react-intl'
import OperatingButton from '@/app/components/common/OperatingButton'
import {DATA_SCHEMA_RERUN_API} from '@/app/containers/ResourceManage/api'
// 导入样式
import styles from './res/styles/index.less'
import Request from "@/app/utils/request";
import {KAFKA_READER_GET_OFFSET_RANGE_API} from "@/app/containers/toolSet/api";

const FormItem = Form.Item
const Option = Select.Option
const TextArea = Input.TextArea

@Form.create()
export default class DataSourceManageRerunModal extends Component {
  constructor (props) {
    super(props)
    this.state = {
      beginOffset: 0,
      endOffset: 0
    }
  }

  componentWillMount = () => {
    const {record} = this.props
    if (record.src_topic) {
      Request(KAFKA_READER_GET_OFFSET_RANGE_API, {
        params: {
          topic: record.src_topic
        },
        method: 'get'
      })
        .then(res => {
          if (res && res.status === 0) {
            const {beginOffset, endOffset} = res.payload
            this.setState({
              beginOffset,
              endOffset
            })
          } else {
            message.warn(res.message)
          }
        })
        .catch(error => {
          error.response && error.response.data && error.response.data.message
            ? message.error(error.response.data.message)
            : message.error(error.message)
        })
    }
  }

  handleSubmit = () => {
    const {onClose, record} = this.props
    this.props.form.validateFieldsAndScroll((err, values) => {
      if (!err) {
        Request(DATA_SCHEMA_RERUN_API, {
          params: {
            dsId: record.ds_id,
            dsName: record.ds_name,
            schemaName: record.schema_name,
            ...values
          },
          method: 'get'
        })
          .then(res => {
            if (res && res.status === 0) {
              message.success(res.message)
              onClose()
            } else {
              message.warn(res.message)
            }
          })
          .catch(error => {
            error.response && error.response.data && error.response.data.message
              ? message.error(error.response.data.message)
              : message.error(error.message)
          })
      }
    })
  }

  render () {
    const { getFieldDecorator } = this.props.form
    const {record, key, visible, onClose} = this.props
    const {beginOffset, endOffset} = this.state
    const formItemLayout = {
      labelCol: {
        xs: { span: 5 },
        sm: { span: 6 }
      },
      wrapperCol: {
        xs: { span: 19 },
        sm: { span: 12 }
      }
    }
    return (
      <div className={styles.table}>
        <Modal
          key={key}
          visible={visible}
          maskClosable={false}
          width={1000}
          title={<FormattedMessage
            id="app.components.projectManage.projectTopology.table.rerun"
            defaultMessage="拖回重跑"
          />}
          onCancel={onClose}
          onOk={this.handleSubmit}
        >
            <Form autoComplete="off">
              <FormItem label="Offset" {...formItemLayout}>
                {getFieldDecorator('offset', {
                  initialValue: beginOffset,
                  rules: [
                    {
                      required: true,
                      message: 'Offset不能为空'
                    },
                    {
                      pattern: /^\d+$/,
                      message: '请输入正确的Offset'
                    }
                  ]
                })(<Input size="default" type="text" />)}
                <font style={{marginLeft: 10}} color="gray">
                  Topic: {record && record.src_topic}，
                  起始Offset：{beginOffset}，
                  末尾Offset（不包含）：{endOffset}
                </font>
              </FormItem>
            </Form>
        </Modal>
      </div>
    )
  }
}

DataSourceManageRerunModal.propTypes = {
}
