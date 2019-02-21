import React, { PropTypes, Component } from 'react'
import { Button, message, Modal, Form, Select, Input } from 'antd'
import { FormattedMessage } from 'react-intl'
import {
  PROJECT_TABLE_GET_INITIAL_LOAD_CONF_API,
  PROJECT_TABLE_SAVE_INITIAL_LOAD_CONF_API
} from '@/app/containers/ProjectManage/api'
// 导入样式
import styles from './res/styles/index.less'
import Request from "@/app/utils/request";

const FormItem = Form.Item
const Option = Select.Option
const TextArea = Input.TextArea

@Form.create()
export default class ProjectTableInitialLoadModal extends Component {
  constructor (props) {
    super(props)
    this.state = {
    }
  }

  componentWillMount = () => {
    const {record} = this.props
    if (record && record.tableId) {
      Request(PROJECT_TABLE_GET_INITIAL_LOAD_CONF_API, {
        params: {
          projectTopoTableId: record.tableId
        },
        method: 'get'
      })
        .then(res => {
          if (res && res.status === 0) {
            this.props.form.setFieldsValue({
              fullpullCol: res.payload.fullpullCol,
              fullpullSplitShardSize: res.payload.fullpullSplitShardSize,
              fullpullSplitStyle: res.payload.fullpullSplitStyle,
              fullpullCondition: res.payload.fullpullCondition,
            })
          } else {
            message.warn(res.message)
          }
        })
        .catch(error => message.error(error))
    }
  }

  handleSave = (ope) => {
    const {record} = this.props
    this.props.form.validateFieldsAndScroll((err, values) => {
      if (!err) {
        Request(PROJECT_TABLE_SAVE_INITIAL_LOAD_CONF_API, {
          data: {
            id: record.tableId,
            tableId: record.sourcetableId,
            fullpullCol: values.fullpullCol,
            fullpullSplitShardSize: values.fullpullSplitShardSize,
            fullpullSplitStyle: values.fullpullSplitStyle,
            fullpullCondition: values.fullpullCondition,
          },
          method: 'post'
        })
          .then(res => {
            if (res && res.status === 0) {
              if (ope === 'onlySave') {
                message.success(res.message)
              } else {
                this.handleSubmit()
              }
              message.success(res.message)
            } else {
              message.warn(res.message)
            }
          })
          .catch(error => message.error(error))
      }
    })
  }

  handleSubmit = () => {
    const {initialLoadApi, onClose, record, onRequest} = this.props
    this.props.form.validateFieldsAndScroll((err, values) => {
      if (!err) {
        onRequest({
          api: initialLoadApi,
          params: {
            ...values,
            projectTableId: record.tableId
          },
          callback: onClose
        })
      }
    })
  }

  render () {
    const { getFieldDecorator } = this.props.form
    const {key, visible, record, onClose} = this.props
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
          maskClosable={true}
          width={1000}
          title={<FormattedMessage
            id="app.components.projectManage.projectTable.fullpull"
            defaultMessage="拉全量"
          />}
          onCancel={onClose}
          footer={[
            <Button onClick={onClose}> 返 回 </Button>,
            <Button type="primary" onClick={() => this.handleSave('onlySave')}> 仅保存全量配置 </Button>,
            <Button type="primary" onClick={() => this.handleSave('saveAndSubmit')}> 确 定 </Button>,
          ]}
        >
          <Form autoComplete="off" onKeyUp={e => {
            e.keyCode === 13 && this.handleSubmit()
          }}>
            <FormItem label={<FormattedMessage
              id="app.components.projectManage.projectTable.outputTopic"
              defaultMessage="输出Topic"
            />} {...formItemLayout}>
              {getFieldDecorator('outputTopic', {
                initialValue: record.outputTopic,
                rules: [
                  {
                    required: true,
                    message: 'topic不能为空'
                  },
                  {
                    pattern: /^\S+$/,
                    message: '请输入正确topic'
                  }
                ]
              })(<Input size="large" type="text" />)}
            </FormItem>
            <FormItem label={'fullpull_col'} {...formItemLayout}>
              {getFieldDecorator('fullpullCol', {
                initialValue: null,
              })(<Input size="large" type="text" />)}
            </FormItem>
            <FormItem label={'fullpull_split_shard_size'} {...formItemLayout}>
              {getFieldDecorator('fullpullSplitShardSize', {
                initialValue: null,
              })(<Input size="large" type="text" />)}
            </FormItem>
            <FormItem label={'fullpull_split_style'} {...formItemLayout}>
              {getFieldDecorator('fullpullSplitStyle', {
                initialValue: null,
              })(<Input size="large" type="text" />)}
            </FormItem>
            <FormItem label={'fullpull_condition'} {...formItemLayout}>
              {getFieldDecorator('fullpullCondition', {
                initialValue: null,
              })(<Input size="large" type="text" />)}
            </FormItem>
          </Form>
        </Modal>
      </div>
    )
  }
}

ProjectTableInitialLoadModal.propTypes = {
}
