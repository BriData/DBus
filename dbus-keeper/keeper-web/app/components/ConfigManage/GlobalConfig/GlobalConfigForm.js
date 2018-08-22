import React, {PropTypes, Component} from 'react'
import {Form, Select, Input, message, Table, Button, Tabs} from 'antd'

const FormItem = Form.Item
const Option = Select.Option

@Form.create()
export default class GlobalConfigForm extends Component {
  constructor(props) {
    super(props)
    this.state = {}
  }

  handleSave = () => {
    const {onSave} = this.props
    this.props.form.validateFieldsAndScroll((err, values) => {
      if (!err) {
        const content = {}
        Object.keys(values).forEach(key => {
          content[key.replace(/@@@/g, '.')] = values[key]
        })
        onSave(content)
      }
    })
  }

  render() {
    const {getFieldDecorator} = this.props.form
    const formItemLayout = {
      labelCol: {
        xs: {span: 4},
        sm: {span: 4}
      },
      wrapperCol: {
        xs: {span: 19},
        sm: {span: 12}
      }
    }
    const tailFormItemLayout = {
      wrapperCol: {
        xs: {
          span: 12,
          offset: 4,
        },
        sm: {
          span: 12,
          offset: 4,
        }
      }
    }

    const {config} = this.props
    return (
      <div>
        <Form className="heartbeat-advance-config-form">
          <FormItem label='Kafka Bootstrap Servers' {...formItemLayout}>
            {getFieldDecorator('bootstrap@@@servers', {
              initialValue: config['bootstrap.servers'],
              rules: [
                {
                  required: true,
                  message: '不能为空'
                }
              ]
            })(
              <Input placeholder="请输入kafka地址" size="large" type="text"/>
            )}
          </FormItem>
          <FormItem label='Kafka Version' {...formItemLayout}>
            {getFieldDecorator('bootstrap@@@servers@@@version', {
              initialValue: config['bootstrap.servers.version'],
              rules: [
                {
                  required: true,
                  message: '不能为空'
                }
              ]
            })(
              <Input placeholder="请输入kafka版本" size="large" type="text"/>
            )}
          </FormItem>
          <FormItem label='Monitor Url' {...formItemLayout}>
            {getFieldDecorator('monitor_url', {
              initialValue: config['monitor_url'],
              rules: [
                {
                  required: true,
                  message: '不能为空'
                }
              ]
            })(
              <Input placeholder="请输入monitor_url" size="large" type="text"/>
            )}
          </FormItem>
          <FormItem label='Influxdb Url' {...formItemLayout}>
            {getFieldDecorator('influxdb_url', {
              initialValue: config['influxdb_url'],
              rules: [
                {
                  required: true,
                  message: '不能为空'
                }
              ]
            })(
              <Input placeholder="请输入Influxdb_url" size="large" type="text"/>
            )}
          </FormItem>
          <FormItem label='Storm Nimbus 主机' {...formItemLayout}>
            {getFieldDecorator('storm@@@nimbus@@@host', {
              initialValue: config['storm.nimbus.host'],
              rules: [
                {
                  required: true,
                  message: '不能为空'
                }
              ]
            })(
              <Input placeholder="请输入storm.nimbus.host" size="large" type="text"/>
            )}
          </FormItem>
          <FormItem label='Storm Nimbus 端口' {...formItemLayout}>
            {getFieldDecorator('storm@@@nimbus@@@port', {
              initialValue: config['storm.nimbus.port'],
              rules: [
                {
                  required: true,
                  message: '不能为空'
                }
              ]
            })(
              <Input placeholder="请输入storm.nimbus.port" size="large" type="text"/>
            )}
          </FormItem>
          <FormItem label='Storm Home 路径' {...formItemLayout}>
            {getFieldDecorator('storm@@@home@@@path', {
              initialValue: config['storm.home.path'],
              rules: [
                {
                  required: true,
                  message: '不能为空'
                }
              ]
            })(
              <Input placeholder="请输入storm.home.path" size="large" type="text"/>
            )}
          </FormItem>
          <FormItem label='免密登录用户名' {...formItemLayout}>
            {getFieldDecorator('user', {
              initialValue: config['user'],
              rules: [
                {
                  required: true,
                  message: '不能为空'
                }
              ]
            })(
              <Input placeholder="请输入免密登录用户名" size="large" type="text"/>
            )}
          </FormItem>
          <FormItem label='Storm Rest API 地址' {...formItemLayout}>
            {getFieldDecorator('stormRest', {
              initialValue: config['stormRest'],
              rules: [
                {
                  required: true,
                  message: '不能为空'
                }
              ]
            })(
              <Input placeholder="请输入stormRest" size="large" type="text"/>
            )}
          </FormItem>
          <FormItem label='ZK地址' {...formItemLayout}>
            {getFieldDecorator('zk@@@url', {
              initialValue: config['zk.url'],
              rules: [
                {
                  required: true,
                  message: '不能为空'
                }
              ]
            })(
              <Input placeholder="请输入ZK地址" size="large" type="text"/>
            )}
          </FormItem>
          <FormItem label='DBus Jar包路径' {...formItemLayout}>
            {getFieldDecorator('dbus@@@jars@@@base@@@path', {
              initialValue: config['dbus.jars.base.path'],
              rules: [
                {
                  required: true,
                  message: '不能为空'
                }
              ]
            })(
              <Input placeholder="请输入DBus Jar包路径" size="large" type="text"/>
            )}
          </FormItem>
          <FormItem label='Router Jar包路径' {...formItemLayout}>
            {getFieldDecorator('dbus@@@router@@@jars@@@base@@@path', {
              initialValue: config['dbus.router.jars.base.path'],
              rules: [
                {
                  required: true,
                  message: '不能为空'
                }
              ]
            })(
              <Input placeholder="请输入Router Jar包路径" size="large" type="text"/>
            )}
          </FormItem>
          <FormItem label='脱敏Jar包路径' {...formItemLayout}>
            {getFieldDecorator('dbus@@@encode@@@plugins@@@jars@@@base@@@path', {
              initialValue: config['dbus.encode.plugins.jars.base.path'],
              rules: [
                {
                  required: true,
                  message: '不能为空'
                }
              ]
            })(
              <Input placeholder="请输入脱敏Jar包路径" size="large" type="text"/>
            )}
          </FormItem>
          <FormItem label='心跳ip地址' {...formItemLayout}>
            {getFieldDecorator('heartbeat@@@host', {
              initialValue: config['heartbeat.host'],
              rules: [
                {
                  required: true,
                  message: '不能为空'
                }
              ]
            })(
              <Input placeholder="请输入心跳ip地址" size="large" type="text"/>
            )}
          </FormItem>
          <FormItem label='心跳机器SSH端口号' {...formItemLayout}>
            {getFieldDecorator('heartbeat@@@port', {
              initialValue: config['heartbeat.port'],
              rules: [
                {
                  required: true,
                  message: '不能为空'
                }
              ]
            })(
              <Input placeholder="请输入SSH端口号" size="large" type="text"/>
            )}
          </FormItem>
          <FormItem label='心跳机器SSH用户名' {...formItemLayout}>
            {getFieldDecorator('heartbeat@@@user', {
              initialValue: config['heartbeat.user'],
              rules: [
                {
                  required: true,
                  message: '不能为空'
                }
              ]
            })(
              <Input placeholder="请输入SSH用户名" size="large" type="text"/>
            )}
          </FormItem>
          <FormItem label='心跳路径' {...formItemLayout}>
            {getFieldDecorator('heartbeat@@@jar@@@path', {
              initialValue: config['heartbeat.jar.path'],
              rules: [
                {
                  required: true,
                  message: '不能为空'
                }
              ]
            })(
              <Input placeholder="请输入心跳路径" size="large" type="text"/>
            )}
          </FormItem>
          <FormItem {...tailFormItemLayout}>
            <Button type="primary" onClick={this.handleSave}>提交</Button>
          </FormItem>
        </Form>
      </div>
    )
  }
}

GlobalConfigForm.propTypes = {}
