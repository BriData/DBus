import React, { PropTypes, Component } from 'react'
import { Modal, Form, Select, Input, Button, message,Table, Spin } from 'antd'
import { FormattedMessage } from 'react-intl'
import OperatingButton from '@/app/components/common/OperatingButton'
import {DATA_TABLE_RERUN_API} from '@/app/containers/ResourceManage/api'
import {KAFKA_READER_GET_OFFSET_RANGE_API} from '@/app/containers/toolSet/api'
// 导入样式
import styles from './res/styles/index.less'
import Request from "@/app/utils/request";

const FormItem = Form.Item
const Option = Select.Option
const TextArea = Input.TextArea

@Form.create()
export default class DataTableManageRerunModal extends Component {
  constructor (props) {
    super(props)
    this.state = {
      beginOffset: 0,
      endOffset: 0
    }
  }

  componentWillMount = () => {
    const {record} = this.props
    if (record.schemaName && record.tableName) {
      Request(KAFKA_READER_GET_OFFSET_RANGE_API, {
        params: {
          topic: `${record.schemaName}.${record.tableName}`
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
        Request(DATA_TABLE_RERUN_API, {
          params: {
            dsId: record.dsId,
            dsName: record.dsName,
            schemaId: record.schemaId,
            schemaName: record.schemaName,
            tableId: record.id,
            tableName: record.tableName,
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
                      // pattern: /^(\d+|start|end)$/,
                      pattern: /^(\d+)$/,
                      message: '请输入正确的Offset'
                    }
                  ]
                })(<Input size="default" type="text" />)}
                <div>
                  <font style={{marginLeft: 10}} color="gray">
                    Topic: {`${record.schemaName}.${record.tableName}`}，
                    起始Offset：{beginOffset}，
                    末尾Offset（不包含）：{endOffset}
                  </font>
                </div>
                {/*<div>
                  <Button onClick={() => this.props.form.setFieldsValue({offset: 'start'})}>Start</Button>
                  <Button onClick={() => this.props.form.setFieldsValue({offset: 'end'})} style={{marginLeft: 10}}>End</Button>
                </div>*/}
              </FormItem>
            </Form>
        </Modal>
      </div>
    )
  }
}

DataTableManageRerunModal.propTypes = {
}
