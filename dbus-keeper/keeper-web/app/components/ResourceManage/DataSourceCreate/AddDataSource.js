import React, {PropTypes, Component} from 'react'
import {Icon, Form, Select, Input, Button, message} from 'antd'
const Textarea = Input.TextArea
// 导入样式
import styles from './res/styles/index.less'
import Request from "@/app/utils/request";
import LOGSTASH from './res/image/logstash.svg'
import UMS from './res/image/ums.jpg'
import FLUME from './res/image/flume.png'
import FILEBEAT from './res/image/filebeat.svg'
const IMAGE_SIZE = 16

const FormItem = Form.Item
const Option = Select.Option

@Form.create()
export default class AddDataSource extends Component {
  constructor(props) {
    super(props)
    this.state = {
      isLogDsType: false
    }
  }

  handleDsNameChange = value => {
    this.props.form.setFieldsValue({
      topic: `${value}`,
      ctrlTopic: `${value}_ctrl`,
      schemaTopic: `${value}_schema`,
      splitTopic: `${value}_split`,
    })
  }

  handleDsTypeChange = value => {
    if (value.startsWith('log_')) {
      this.setState({isLogDsType: true})
      this.props.form.setFieldsValue({
        dbusUser: 'empty',
        dbusPwd: 'empty',
        masterUrl: 'empty',
        slaveUrl: 'empty'
      })
    } else {
      this.setState({isLogDsType: false})
      this.props.form.setFieldsValue({
        dbusUser: null,
        dbusPwd: null,
        masterUrl: null,
        slaveUrl: null
      })
    }
  }

  handleNext = () => {
    this.props.form.validateFieldsAndScroll((err, values) => {
      if (!err) {
        this.handleValidate({...values})
      }
    })
  }

  // 检查数据源连通性（非log类型）
  handleValidate = values => {
    const {validateDataSourceApi} = this.props
    Request(validateDataSourceApi, {
      data: {
        dsType: values.dsType,
        masterUrl: values.masterUrl,
        slaveUrl: values.slaveUrl,
        user: values.dbusUser,
        pwd: values.dbusPwd
      },
      method: 'post'
    })
      .then(res => {
        if (res && res.status === 0) {
          this.handleInsert(values)
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

  // 插入管理库
  handleInsert = values => {
    const {addDataSourceApi} = this.props
    Request(addDataSourceApi, {
      data: values,
      method: 'post'
    })
      .then(res => {
        if (res && res.status === 0) {
          values = {
            id: res.payload,
            ...values
          }
          this.handleGotoStep2(values)
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

  // 跳转到schema的Tab页面
  handleGotoStep2 = values => {
    console.info("新数据源", values)
    const {onMoveTab} = this.props
    onMoveTab({
      dataSource: values
    })
  }

  render() {
    const {getFieldDecorator} = this.props.form
    const formItemLayout = {
      labelCol: {
        span: 4
      },
      wrapperCol: {
        span: 12
      }
    }
    const tailFormItemLayout = {
      wrapperCol: {
        span: 12,
        offset: 4,
      }
    }
    const {isLogDsType} = this.state

    const imageProps = {style: {verticalAlign: 'middle'}}
    return (
      <div>
        <Form className='data-source-start-topo-form'>
          <FormItem label='数据源名称' {...formItemLayout}>
            {getFieldDecorator('dsName', {
              initialValue: null,
              rules: [
                {
                  required: true,
                  message: '不能为空'
                },
                {
                  pattern: /^[a-zA-Z0-9_]+$/,
                  message: '仅支持字母、数字、下划线'
                }
              ]
            })(
              <Input onChange={e => this.handleDsNameChange(e.target.value)} placeholder="数据源名称" size="large" type="text"/>
            )}
          </FormItem>
          <FormItem label='数据源类型' {...formItemLayout}>
            {getFieldDecorator('dsType', {
              initialValue: null,
              rules: [
                {
                  required: true,
                  message: '不能为空'
                }
              ]
            })(
              <Select
                showSearch
                optionFilterProp='children'
                placeholder="数据源类型"
                style={{ width: '100%' }}
                onChange={this.handleDsTypeChange}
              >
                <Option key="oracle" value="oracle"><Icon style={{color: 'red', verticalAlign: 'middle', height: IMAGE_SIZE, width: IMAGE_SIZE}} type="database" /> oracle</Option>
                <Option key="mysql" value="mysql"><Icon style={{color: 'blue', verticalAlign: 'middle', height: IMAGE_SIZE, width: IMAGE_SIZE}} type="database" /> mysql</Option>
                <Option key="log_logstash" value="log_logstash"><img {...imageProps} height={IMAGE_SIZE} width={IMAGE_SIZE} src={LOGSTASH}/> log_logstash</Option>
                <Option key="log_logstash_json" value="log_logstash_json"><img {...imageProps} height={IMAGE_SIZE} width={IMAGE_SIZE} src={LOGSTASH}/> log_logstash_json</Option>
                <Option key="log_ums" value="log_ums"><img {...imageProps} height={IMAGE_SIZE} width={IMAGE_SIZE} src={UMS}/> log_ums</Option>
                <Option key="log_flume" value="log_flume"><img {...imageProps} height={IMAGE_SIZE} width={IMAGE_SIZE} src={FLUME}/> log_flume</Option>
                <Option key="log_filebeat" value="log_filebeat"><img {...imageProps} height={IMAGE_SIZE} width={IMAGE_SIZE} src={FILEBEAT}/> log_filebeat</Option>
              </Select>
            )}
          </FormItem>
          <FormItem label='状态' {...formItemLayout}>
            {getFieldDecorator('status', {
              initialValue: null,
              rules: [
                {
                  required: true,
                  message: '不能为空'
                }
              ]
            })(
              <Select
                showSearch
                optionFilterProp='children'
                placeholder="状态"
                style={{ width: '100%' }}
              >
                <Option key="active" value="active">active</Option>
                <Option key="inactive" value="inactive">inactive</Option>
              </Select>
            )}
          </FormItem>
          <FormItem label='描述' {...formItemLayout}>
            {getFieldDecorator('dsDesc', {
              initialValue: null,
              rules: [
                {
                  required: true,
                  message: '不能为空'
                }
              ]
            })(
              <Input placeholder="描述" size="large" type="text"/>
            )}
          </FormItem>
          <FormItem label='用户名' {...formItemLayout}>
            {getFieldDecorator('dbusUser', {
              initialValue: null,
              rules: [
                {
                  required: true,
                  message: '不能为空'
                },
                {
                  pattern: /^[a-zA-Z0-9_]+$/,
                  message: '仅支持字母、数字、下划线'
                }
              ]
            })(
              <Input disabled={isLogDsType} placeholder="用户名" size="large" type="text"/>
            )}
          </FormItem>
          <FormItem label='密码' {...formItemLayout}>
            {getFieldDecorator('dbusPwd', {
              initialValue: null,
              rules: [
                {
                  required: true,
                  message: '不能为空'
                }
              ]
            })(
              <Input disabled={isLogDsType} placeholder="密码" type="password" size="large"/>
            )}
          </FormItem>
          <FormItem label='主库URL' {...formItemLayout}>
            {getFieldDecorator('masterUrl', {
              initialValue: null,
              rules: [
                {
                  required: true,
                  message: '不能为空'
                }
              ]
            })(
              <Textarea disabled={isLogDsType} autosize={{minRows:3}} placeholder="主库URL" type="text" size="large"/>
            )}
          </FormItem>
          <FormItem label='从库URL' {...formItemLayout}>
            {getFieldDecorator('slaveUrl', {
              initialValue: null,
              rules: [
                {
                  required: true,
                  message: '不能为空'
                }
              ]
            })(
              <Textarea disabled={isLogDsType} autosize={{minRows:3}} placeholder="从库URL" type="text" size="large"/>
            )}
          </FormItem>
          <FormItem label='Topic' {...formItemLayout}>
            {getFieldDecorator('topic', {
              initialValue: null,
              rules: [
                {
                  required: true,
                  message: '不能为空'
                }
              ]
            })(
              <Input placeholder="Topic" size="large" type="text"/>
            )}
          </FormItem>
          <FormItem label='CtrlTopic' {...formItemLayout}>
            {getFieldDecorator('ctrlTopic', {
              initialValue: null,
              rules: [
                {
                  required: true,
                  message: '不能为空'
                }
              ]
            })(
              <Input placeholder="CtrlTopic" size="large" type="text"/>
            )}
          </FormItem>
          <FormItem label='SchemaTopic' {...formItemLayout}>
            {getFieldDecorator('schemaTopic', {
              initialValue: null,
              rules: [
                {
                  required: true,
                  message: '不能为空'
                }
              ]
            })(
              <Input placeholder="SchemaTopic" size="large" type="text"/>
            )}
          </FormItem>
          <FormItem label='SplitTopic' {...formItemLayout}>
            {getFieldDecorator('splitTopic', {
              initialValue: null,
              rules: [
                {
                  required: true,
                  message: '不能为空'
                }
              ]
            })(
              <Input placeholder="SplitTopic" size="large" type="text"/>
            )}
          </FormItem>
          <FormItem {...tailFormItemLayout}>
            <Button type="primary" onClick={this.handleNext}>下一步</Button>
          </FormItem>
        </Form>
      </div>
    )
  }
}

AddDataSource.propTypes = {}
