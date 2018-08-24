import React, { PropTypes, Component } from 'react'
import { Modal, Form, Select, Input, Button, message,Table, Spin } from 'antd'
import { FormattedMessage } from 'react-intl'
import OperatingButton from '@/app/components/common/OperatingButton'

// 导入样式
import styles from './res/styles/index.less'
import Request from "@/app/utils/request";

const FormItem = Form.Item
const Option = Select.Option
const TextArea = Input.TextArea

@Form.create()
export default class DataSourceManageModifyModal extends Component {
  constructor (props) {
    super(props)
    this.state = {
    }
  }

  handleSubmit = () => {
    const {updateApi} = this.props
    const {onClose} = this.props
    this.props.form.validateFieldsAndScroll((err, values) => {
      if (!err) {
        Request(updateApi, {
          data: {
            ...values,
            updateTime: undefined
          },
          method: 'post'
        })
          .then(res => {
            if (res && res.status === 0) {
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
    const {key, visible, dsInfo, onClose} = this.props
    const {result} = dsInfo
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
          className="top-modal"
          key={key}
          visible={visible}
          maskClosable={false}
          width={1000}
          title={'修改数据源基本信息'}
          onCancel={onClose}
          onOk={this.handleSubmit}
        >
          <Spin spinning={dsInfo.loading} tip="正在加载数据中...">
            {!dsInfo.loading ? (
              <Form autoComplete="off" className="data-source-modify-form">
                <FormItem label="ID" {...formItemLayout}>
                  {getFieldDecorator('id', {
                    initialValue: result.id,
                  })(<Input disabled={true} size="default" type="text" />)}
                </FormItem>
                <FormItem label="Name" {...formItemLayout}>
                  {getFieldDecorator('dsName', {
                    initialValue: result.dsName,
                  })(<Input disabled={true} size="default" type="text" />)}
                </FormItem>
                <FormItem label="Type" {...formItemLayout}>
                  {getFieldDecorator('dsType', {
                    initialValue: result.dsType,
                  })(<Input disabled={true} size="default" type="text" />)}
                </FormItem>
                <FormItem
                  label={"Status"} {...formItemLayout}
                >
                  {getFieldDecorator('status', {
                    initialValue:result.status
                  })(
                    <Select
                      showSearch
                      optionFilterProp='children'
                      className={styles.select}
                      placeholder="Select status"
                    >
                      <Option value="active" key="active">active</Option>
                      <Option value="inactive" key="inactive">inactive</Option>
                    </Select>
                  )}
                </FormItem>
                <FormItem label="Desc" {...formItemLayout}>
                  {getFieldDecorator('dsDesc', {
                    initialValue: result.dsDesc,
                  })(<TextArea autosize={true}/>)}
                </FormItem>
                <FormItem label="DB Master URL" {...formItemLayout}>
                  {getFieldDecorator('masterUrl', {
                    initialValue: result.masterUrl
                  })(<TextArea autosize={true}/>)}
                </FormItem>
                <FormItem label="DB Slave URL" {...formItemLayout}>
                  {getFieldDecorator('slaveUrl', {
                    initialValue: result.slaveUrl
                  })(<TextArea autosize={true}/>)}
                </FormItem>
                <FormItem label="DS Partition" {...formItemLayout}>
                  {getFieldDecorator('dsPartition', {
                    initialValue: result.dsPartition
                  })(<Input size="default" type="text" />)}
                </FormItem>
                <FormItem label="Topic" {...formItemLayout}>
                  {getFieldDecorator('topic', {
                    initialValue: result.topic,
                  })(<Input disabled={true} size="default" type="text" />)}
                </FormItem>
                <FormItem label="Ctrl Topic" {...formItemLayout}>
                  {getFieldDecorator('ctrlTopic', {
                    initialValue: result.ctrlTopic,
                  })(<Input disabled={true} size="default" type="text" />)}
                </FormItem>
                <FormItem label="Schema Topic" {...formItemLayout}>
                  {getFieldDecorator('schemaTopic', {
                    initialValue: result.schemaTopic,
                  })(<Input disabled={true} size="default" type="text" />)}
                </FormItem>
                <FormItem label="Split Topic" {...formItemLayout}>
                  {getFieldDecorator('splitTopic', {
                    initialValue: result.splitTopic,
                  })(<Input disabled={true} size="default" type="text" />)}
                </FormItem>
                <FormItem label="DBus User" {...formItemLayout}>
                  {getFieldDecorator('dbusUser', {
                    initialValue: result.dbusUser,
                  })(<Input disabled={true} size="default" type="text" />)}
                </FormItem>
                <FormItem label="上次修改时间" {...formItemLayout}>
                  {getFieldDecorator('updateTime', {
                    initialValue: result.updateTime
                  })(<Input disabled={true} size="default" type="text" />)}
                </FormItem>
              </Form>
            ) : (
              <div style={{ height: '378px' }} />
            )}
          </Spin>
        </Modal>
      </div>
    )
  }
}

DataSourceManageModifyModal.propTypes = {
}
