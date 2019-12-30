import React, {Component} from 'react'
import {Button, Form, Input, message, Modal, Select} from 'antd'
import {FormattedMessage} from 'react-intl'
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
  constructor(props) {
    super(props)
    this.state = {
      loading: false
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
              rows: res.payload.rows
            })
          } else {
            message.warn(res.message)
          }
        })
        .catch(error => message.error(error))
    }
  }

  handleSave = (ope) => {
    const {record, onClose} = this.props
    this.props.form.validateFieldsAndScroll((err, values) => {
      if (!err) {
        Request(PROJECT_TABLE_SAVE_INITIAL_LOAD_CONF_API, {
          data: {
            id: record.tableId,
            tableId: record.sourcetableId,
            fullpullCol: values.fullpullCol,
            fullpullSplitShardSize: values.fullpullSplitShardSize,
            fullpullSplitStyle: values.fullpullSplitStyle === undefined ? '' : values.fullpullSplitStyle,
            fullpullCondition: values.fullpullCondition
          },
          method: 'post'
        })
          .then(res => {
            if (res && res.status === 0) {
              if (ope === 'onlySave') {
                message.success(res.message)
                onClose()
              } else {
                this.handleSubmit(values)
              }
            } else {
              message.warn(res.message)
            }
          })
          .catch(error => message.error(error))
      }
    })
  }

  handleSubmit = (values) => {
    this.setState({loading: true})
    const {initialLoadApi, record, onClose} = this.props
    Request(initialLoadApi, {
      params: {
        ...values,
        projectTableId: record.tableId
      },
      method: 'get'
    })
      .then(res => {
        this.setState({loading: false})
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

  render() {
    const {getFieldDecorator} = this.props.form
    const {key, visible, record, onClose} = this.props
    const {loading} = this.state
    const formItemLayout = {
      labelCol: {
        xs: {span: 5},
        sm: {span: 6}
      },
      wrapperCol: {
        xs: {span: 19},
        sm: {span: 12}
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
            <Button type="primary" loading={loading} onClick={() => this.handleSave('saveAndSubmit')}> 确 定 </Button>,
          ]}
        >
          <Form autoComplete="off" onKeyUp={e => {
            e.keyCode === 13 && this.handleSubmit()
          }}>
            <FormItem label={'总数据量'} {...formItemLayout}>
              {getFieldDecorator('rows', {
                initialValue: null,
              })(<Input disabled={true} size="large" type="text"/>)}
            </FormItem>
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
              })(<Input size="large" type="text"/>)}
            </FormItem>
            <FormItem label={'分片列'} {...formItemLayout}>
              {getFieldDecorator('fullpullCol', {
                initialValue: null,
              })(<Input size="large" type="text"/>)}
            </FormItem>
            <FormItem label={'分片大小'} {...formItemLayout}>
              {getFieldDecorator('fullpullSplitShardSize', {
                initialValue: null,
              })(<Input size="large" type="text"/>)}
            </FormItem>
            <FormItem label={'分片类型'} {...formItemLayout}>
              {getFieldDecorator('fullpullSplitStyle', {
                initialValue: null,
              })(
                <Select
                  showSearch
                  optionFilterProp="children"
                  allowClear={true}
                >
                  <Option value="number">number</Option>
                  <Option value="md5">md5</Option>
                  <Option value="md5big">md5big</Option>
                  <Option value="uuid">uuid</Option>
                  <Option value="uuidbig">uuidbig</Option>
                </Select>
              )}
            </FormItem>
            <FormItem label={'分片条件'} {...formItemLayout}>
              {getFieldDecorator('fullpullCondition', {
                initialValue: null,
              })(<Input size="large" type="text"/>)}
            </FormItem>
          </Form>
        </Modal>
      </div>
    )
  }
}

ProjectTableInitialLoadModal.propTypes = {}
