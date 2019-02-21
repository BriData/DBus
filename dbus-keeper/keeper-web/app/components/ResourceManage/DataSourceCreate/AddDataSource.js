import React, {PropTypes, Component} from 'react'
import {Checkbox, Icon, Form, Select, Input, Button, message} from 'antd'
const Textarea = Input.TextArea
// 导入样式
import styles from './res/styles/index.less'
import Request from "@/app/utils/request";
import LOGSTASH from './res/image/logstash.svg'
import UMS from './res/image/ums.jpg'
import FLUME from './res/image/flume.png'
import FILEBEAT from './res/image/filebeat.svg'

import {
  DATA_SOURCE_GET_OGG_CONF_API,
  DATA_SOURCE_SET_OGG_CONF_API,
  DATA_SOURCE_GET_CANAL_CONF_API,
  DATA_SOURCE_SET_CANAL_CONF_API
} from '@/app/containers/ResourceManage/api'
const IMAGE_SIZE = 16

const FormItem = Form.Item
const Option = Select.Option


@Form.create()
export default class AddDataSource extends Component {
  constructor(props) {
    super(props)
    this.state = {
      isLogDsType: false,
      isDeployCanal: true,
      isDeployOgg: true,
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

  handleDeployCanalChange = e => {
    this.handleDsTypeChange('mysql')
    this.setState({
      isDeployCanal: e.target.checked
    })
  }

  handleDeployOggChange = e => {
    this.handleDsTypeChange('oracle')
    this.setState({
      isDeployOgg: e.target.checked
    })
  }

  handleDsTypeChange = value => {
    this.setState({}, () => this.props.form.validateFields(null, {force: true}))
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
    }

    const fieldsValue = this.props.form.getFieldsValue()

    if (value === 'oracle') {
      Request(DATA_SOURCE_GET_OGG_CONF_API, {
        params: {
          dsName: fieldsValue.dsName
        },
        method: 'get'
      })
        .then(res => {
          if (res && res.status === 0) {
            this.props.form.setFieldsValue({
              oggHost: res.payload.host || res.payload.hosts,
              oggUser: res.payload.user,
              oggPort: res.payload.port,
              oggToolPath: res.payload.oggToolPath,
              oggPath: res.payload.oggPath,
              trailName: res.payload.trailName,
              replicatName: res.payload.replicatName
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
    } else if (value === 'mysql') {
      Request(DATA_SOURCE_GET_CANAL_CONF_API, {
        params: {
          dsName: fieldsValue.dsName
        },
        method: 'get'
      })
        .then(res => {
          if (res && res.status === 0) {
            this.props.form.setFieldsValue({
              canalSSHHost: res.payload.hosts,
              canalSSHUser: res.payload.user,
              canalSSHPort: res.payload.port,
              canalPath: res.payload.canalPath,
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
          /**
           * 只有选择了oracle且部署ogg
           * 或者选择了mysql且部署canal
           * 才能自动部署
          */
          const {isDeployCanal, isDeployOgg} = this.state
          if (values.dsType === 'mysql' && isDeployCanal) {
            this.handleSetOggOrCanal(values)
          } else if (values.dsType === 'oracle' && isDeployOgg) {
            this.handleSetOggOrCanal(values)
          } else {
            this.handleInsert(values)
          }
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

  handleSetOggOrCanal = values => {
    if (values.dsType === 'oracle' || values.dsType === 'mysql') {
      let data, api
      if (values.dsType === 'oracle') {
        data = {
          user: values.oggUser,
          port: values.oggPort,
          oggToolPath: values.oggToolPath,
          oggPath: values.oggPath,
          host: values.oggHost,
          replicatName: values.replicatName,
          trailName: values.trailName,
          dsName: values.dsName,
        }
        api = DATA_SOURCE_SET_OGG_CONF_API
      } else if (values.dsType === 'mysql') {
        data = {
          user: values.canalSSHUser,
          port: values.canalSSHPort,
          canalPath: values.canalPath,
          canalAdd: values.canalAdd,
          host: values.canalSSHHost,
          dsName: values.dsName,
        }
        api = DATA_SOURCE_SET_CANAL_CONF_API
      }
      Request(api, {
        data: data,
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

    } else {
      this.handleInsert(values)
    }
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

    const fieldsValue = this.props.form.getFieldsValue()

    const imageProps = {style: {verticalAlign: 'middle'}}

    const normalNameValidate = [
      {
        required: true,
        message: '不能为空'
      },
      {
        pattern: /^[a-zA-Z0-9_]+$/,
        message: '仅支持字母、数字、下划线'
      }
    ]
    const mysqlNameValidate = [
      ...normalNameValidate,
      {
        pattern: /^[a-zA-Z0-9]*_?[a-zA-Z0-9]*$/,
        message: 'mysql数据源名称最多包含1个下划线'
      }
    ]
    const {isDeployCanal, isDeployOgg} = this.state

    return (
      <div>
        <Form autoComplete="off" className='data-source-start-topo-form'>
          <FormItem label='数据源名称' {...formItemLayout}>
            {getFieldDecorator('dsName', {
              initialValue: null,
              rules: this.props.form.getFieldsValue().dsType === 'mysql' ? mysqlNameValidate : normalNameValidate
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


          {/* oracle ogg 表单 */}
          {fieldsValue.dsType === 'oracle' && (
            <FormItem label='是否部署Ogg' {...formItemLayout}>
              <Checkbox
                checked={isDeployOgg}
                onChange={this.handleDeployOggChange}
              />
            </FormItem> )}
          {fieldsValue.dsType === 'oracle' && isDeployOgg && (
            <FormItem label='OGG Trail Prefix' {...formItemLayout}>
              {getFieldDecorator('trailName', {
                initialValue: null,
                rules: [
                  {
                    required: true,
                    message: '不能为空'
                  }
                ]
              })(
                <Input placeholder="aa" size="large" type="text"/>
              )}
            </FormItem>)}
          {fieldsValue.dsType === 'oracle' && isDeployOgg && (
            <FormItem label='Replicat Name' {...formItemLayout}>
              {getFieldDecorator('replicatName', {
                initialValue: null,
                rules: [
                  {
                    required: true,
                    message: '不能为空'
                  },
                  {
                    pattern: /^[A-Za-z0-9_]{1,8}$/,
                    message: '不能超过8个字符，不能含有特殊符号'
                  }
                ]
              })(
                <Input placeholder="oratest" size="large" type="text"/>
              )}
            </FormItem>)}

          {fieldsValue.dsType === 'oracle' && isDeployOgg && (
            <FormItem label='OGG部署服务器Host' {...formItemLayout}>
              {getFieldDecorator('oggHost', {
                initialValue: null,
                rules: [
                  {
                    required: true,
                    message: '不能为空'
                  }
                ]
              })(
                <Input placeholder="dbus-n1" size="large" type="text"/>
              )}
            </FormItem> )}
          {fieldsValue.dsType === 'oracle' && isDeployOgg && (
            <FormItem label='OGG部署服务器SSH用户名' {...formItemLayout}>
              {getFieldDecorator('oggUser', {
                initialValue: null,
                rules: [
                  {
                    required: true,
                    message: '不能为空'
                  }
                ]
              })(
                <Input placeholder="app" size="large" type="text"/>
              )}
            </FormItem> )}
          {fieldsValue.dsType === 'oracle' && isDeployOgg && (
            <FormItem label='OGG部署服务器SSH端口' {...formItemLayout}>
              {getFieldDecorator('oggPort', {
                initialValue: null,
                rules: [
                  {
                    required: true,
                    message: '不能为空'
                  }
                ]
              })(
                <Input placeholder="22" size="large" type="text"/>
              )}
            </FormItem> )}
          {fieldsValue.dsType === 'oracle' && isDeployOgg && (
            <FormItem label='OGG部署小工具目录' {...formItemLayout}>
              {getFieldDecorator('oggToolPath', {
                initialValue: null,
                rules: [
                  {
                    required: true,
                    message: '不能为空'
                  }
                ]
              })(
                <Input placeholder="OGG Tool Path" size="large" type="text"/>
              )}
            </FormItem> )}
          {fieldsValue.dsType === 'oracle' && isDeployOgg && (
            <FormItem label='OGG根目录' {...formItemLayout}>
              {getFieldDecorator('oggPath', {
                initialValue: null,
                rules: [
                  {
                    required: true,
                    message: '不能为空'
                  }
                ]
              })(
                <Input placeholder="OGG Path" size="large" type="text"/>
              )}
            </FormItem> )}

          {fieldsValue.dsType === 'oracle' && isDeployOgg && (
            <FormItem label='NLS_LANG' {...formItemLayout}>
              {getFieldDecorator('NLS_LANG', {
                initialValue: 'AMERICAN_AMERICA.AL32UTF8',
                rules: [
                  {
                    required: true,
                    message: '不能为空'
                  }
                ]
              })(
                <Input placeholder="NLS_LANG" size="large" type="text"/>
              )}
            </FormItem> )}

          {/* mysql canal表单 */}
          {fieldsValue.dsType === 'mysql' && (
            <FormItem label='是否部署Canal' {...formItemLayout}>
              <Checkbox
                checked={isDeployCanal}
                onChange={this.handleDeployCanalChange}
              />
            </FormItem> )}
          {fieldsValue.dsType === 'mysql' && isDeployCanal && (
            <FormItem label='从库Ip:Port' {...formItemLayout}>
              {getFieldDecorator('canalAdd', {
                initialValue: 'dbus-n1:3306',
                rules: [
                  {
                    required: true,
                    message: '不能为空'
                  }
                ]
              })(
                <Input placeholder="Canal Slave" size="large" type="text"/>
              )}
            </FormItem> )}
          {fieldsValue.dsType === 'mysql' && isDeployCanal && (
            <FormItem label='从库Canal用户名' {...formItemLayout}>
              {getFieldDecorator('canalUser', {
                initialValue: 'canal',
                rules: [
                  {
                    required: true,
                    message: '不能为空'
                  }
                ]
              })(
                <Input placeholder="Canal User" size="large" type="text"/>
              )}
            </FormItem> )}
          {fieldsValue.dsType === 'mysql' && isDeployCanal && (
            <FormItem label='从库Canal密码' {...formItemLayout}>
              {getFieldDecorator('canalPass', {
                initialValue: 'canal',
                rules: [
                  {
                    required: true,
                    message: '不能为空'
                  }
                ]
              })(
                <Input placeholder="Canal Password" size="large" type="password"/>
              )}
            </FormItem> )}
          {fieldsValue.dsType === 'mysql' && isDeployCanal && (
            <FormItem label='Canal部署服务器Host' {...formItemLayout}>
              {getFieldDecorator('canalSSHHost', {
                initialValue: null,
                rules: [
                  {
                    required: true,
                    message: '不能为空'
                  }
                ]
              })(
                <Input placeholder="dbus-n1" size="large" type="text"/>
              )}
            </FormItem> )}
          {fieldsValue.dsType === 'mysql' && isDeployCanal && (
            <FormItem label='Canal部署服务器SSH用户名' {...formItemLayout}>
              {getFieldDecorator('canalSSHUser', {
                initialValue: null,
                rules: [
                  {
                    required: true,
                    message: '不能为空'
                  }
                ]
              })(
                <Input placeholder="app" size="large" type="text"/>
              )}
            </FormItem> )}
          {fieldsValue.dsType === 'mysql' && isDeployCanal && (
            <FormItem label='Canal部署服务器SSH端口' {...formItemLayout}>
              {getFieldDecorator('canalSSHPort', {
                initialValue: null,
                rules: [
                  {
                    required: true,
                    message: '不能为空'
                  }
                ]
              })(
                <Input placeholder="22" size="large" type="text"/>
              )}
            </FormItem> )}
          {fieldsValue.dsType === 'mysql' && isDeployCanal && (
            <FormItem label='Canal部署小工具目录' {...formItemLayout}>
              {getFieldDecorator('canalPath', {
                initialValue: null,
                rules: [
                  {
                    required: true,
                    message: '不能为空'
                  }
                ]
              })(
                <Input placeholder="Canal Tool Path" size="large" type="text"/>
              )}
            </FormItem> )}

            {/* 几乎不需要手工配置的几个设置*/}
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
