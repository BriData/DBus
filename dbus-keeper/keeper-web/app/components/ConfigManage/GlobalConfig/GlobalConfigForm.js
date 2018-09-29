import React, {PropTypes, Component} from 'react'
import {Form, Select, Popconfirm, Input, message, Table, Button, Tabs} from 'antd'
import { FormattedMessage } from 'react-intl'
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

  handleInit = options => {
    const {onInit} = this.props
    this.props.form.validateFieldsAndScroll((err, values) => {
      if (!err) {
        const content = {}
        Object.keys(values).forEach(key => {
          content[key.replace(/@@@/g, '.')] = values[key]
        })
        onInit(options, content)
      }
    })
  }

  handleInitGrafana = () => {
    this.handleInit('grafana')
  }

  handleInitInfluxdb = () => {
    this.handleInit('influxdb')
  }

  handleInitStorm = () => {
    this.handleInit('storm')
  }

  handleInitHeartbeat = () => {
    this.handleInit('heartBeat')
  }

  handleInitZk = () => {
    this.handleInit('zk')
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
          <FormItem label={<FormattedMessage id="app.components.configCenter.globalConfig.bootstrapServers" defaultMessage="Kafka 服务器" />} {...formItemLayout}>
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
          <FormItem label={<FormattedMessage id="app.components.configCenter.globalConfig.bootstrapServersVersion" defaultMessage="Kafka 版本" />} {...formItemLayout}>
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
          <FormItem label={<FormattedMessage id="app.components.configCenter.globalConfig.grafanaUrl" defaultMessage="Grafana 外网网址" />} {...formItemLayout}>
            {getFieldDecorator('grafana_url_web', {
              initialValue: config['grafana_url_web'],
              rules: [
                {
                  required: true,
                  message: '不能为空'
                }
              ]
            })(
              <Input placeholder="Grafana Url" size="large" type="text"/>
            )}
          </FormItem>
          <FormItem label={<FormattedMessage id="app.components.configCenter.globalConfig.grafanaInnerUrl" defaultMessage="Grafana 内网网址" />} {...formItemLayout}>
            {getFieldDecorator('grafana_url_dbus', {
              initialValue: config['grafana_url_dbus'],
              rules: [
                {
                  required: true,
                  message: '不能为空'
                }
              ]
            })(
              <Input placeholder="Grafana Url" size="large" type="text"/>
            )}
          </FormItem>
          <FormItem label='Grafana Token' {...formItemLayout}>
            {getFieldDecorator('grafanaToken', {
              initialValue: config['grafanaToken'],
              rules: [
                {
                  required: true,
                  message: '不能为空'
                }
              ]
            })(
              <Input placeholder="请输入Grafana Token" size="large" type="text"/>
            )}
          </FormItem>
          <FormItem {...tailFormItemLayout}>
            <Popconfirm title={'确认初始化Grafana？'} onConfirm={this.handleInitGrafana} okText="Yes" cancelText="No">
              <Button type="danger">
                <FormattedMessage
                  id="app.components.configCenter.globalConfig.initGrafana"
                  defaultMessage="初始化Grafana"
                />
              </Button>
            </Popconfirm>
          </FormItem>
          <FormItem label={<FormattedMessage id="app.components.configCenter.globalConfig.influxdbUrl" defaultMessage="Influxdb 外网网址" />} {...formItemLayout}>
            {getFieldDecorator('influxdb_url_web', {
              initialValue: config['influxdb_url_web'],
              rules: [
                {
                  required: true,
                  message: '不能为空'
                }
              ]
            })(
              <Input placeholder="Influxdb Url" size="large" type="text"/>
            )}
          </FormItem>
          <FormItem label={<FormattedMessage id="app.components.configCenter.globalConfig.influxdbInnerUrl" defaultMessage="Influxdb 内网网址" />} {...formItemLayout}>
            {getFieldDecorator('influxdb_url_dbus', {
              initialValue: config['influxdb_url_dbus'],
              rules: [
                {
                  required: true,
                  message: '不能为空'
                }
              ]
            })(
              <Input placeholder="Influxdb Url" size="large" type="text"/>
            )}
          </FormItem>
          <FormItem {...tailFormItemLayout}>
            <Popconfirm title={'确认初始化Influxdb？'} onConfirm={this.handleInitInfluxdb} okText="Yes" cancelText="No">
              <Button type="danger">
                <FormattedMessage
                  id="app.components.configCenter.globalConfig.initInfluxdb"
                  defaultMessage="初始化Influxdb"
                />
              </Button>
            </Popconfirm>
          </FormItem>
          <FormItem label={<FormattedMessage id="app.components.configCenter.globalConfig.stormNimbusHost" defaultMessage="Storm Nimbus 主机" />} {...formItemLayout}>
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
          <FormItem label={<FormattedMessage
            id="app.components.configCenter.globalConfig.stormNimbusPort"
            defaultMessage="Storm Nimbus 端口"
          />} {...formItemLayout}>
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
          <FormItem label={<FormattedMessage
            id="app.components.configCenter.globalConfig.stormHomePath"
            defaultMessage="Storm 根目录"
          />} {...formItemLayout}>
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
          <FormItem label={<FormattedMessage
            id="app.components.configCenter.globalConfig.sshUser"
            defaultMessage="SSH 免密用户名"
          />} {...formItemLayout}>
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
          <FormItem label={<FormattedMessage
            id="app.components.configCenter.globalConfig.stormUIRestApi"
            defaultMessage="Storm UI 接口地址"
          />} {...formItemLayout}>
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
          <FormItem {...tailFormItemLayout}>
            <Popconfirm title={'确认初始化Storm？'} onConfirm={this.handleInitStorm} okText="Yes" cancelText="No">
              <Button type="danger">
                <FormattedMessage
                  id="app.components.configCenter.globalConfig.initStorm"
                  defaultMessage="初始化Storm"
                />
              </Button>
            </Popconfirm>
          </FormItem>
          <FormItem label={<FormattedMessage
            id="app.components.configCenter.globalConfig.zkServers"
            defaultMessage="ZK 服务器"
          />} {...formItemLayout}>
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
          <FormItem {...tailFormItemLayout}>
            <Popconfirm title={'确认初始化ZK？'} onConfirm={this.handleInitZk} okText="Yes" cancelText="No">
              <Button type="danger">
                <FormattedMessage
                  id="app.components.configCenter.globalConfig.initZk"
                  defaultMessage="初始化ZK"
                />
              </Button>
            </Popconfirm>
          </FormItem>
          <FormItem label={<FormattedMessage
            id="app.components.configCenter.globalConfig.dbusJarPath"
            defaultMessage="DBus Jar包路径"
          />} {...formItemLayout}>
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
          <FormItem label={<FormattedMessage
            id="app.components.configCenter.globalConfig.routerJarPath"
            defaultMessage="Router Jar包路径"
          />} {...formItemLayout}>
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
          <FormItem label={<FormattedMessage
            id="app.components.configCenter.globalConfig.encoderPluginPath"
            defaultMessage="脱敏插件路径"
          />} {...formItemLayout}>
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
          <FormItem label={<FormattedMessage
            id="app.components.configCenter.globalConfig.heartbeatIP"
            defaultMessage="心跳IP地址"
          />} {...formItemLayout}>
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
          <FormItem label={<FormattedMessage
            id="app.components.configCenter.globalConfig.heartbeatSSHPort"
            defaultMessage="心跳服务器SSH端口"
          />} {...formItemLayout}>
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
          <FormItem label={<FormattedMessage
            id="app.components.configCenter.globalConfig.heartbeatSSHUser"
            defaultMessage="心跳服务器SSH用户名"
          />} {...formItemLayout}>
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
          <FormItem label={<FormattedMessage
            id="app.components.configCenter.globalConfig.heartbeatPath"
            defaultMessage="心跳根目录"
          />} {...formItemLayout}>
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
            <Popconfirm title={'确认初始化心跳？'} onConfirm={this.handleInitHeartbeat} okText="Yes" cancelText="No">
              <Button type="danger">
                <FormattedMessage
                  id="app.components.configCenter.globalConfig.initHeartbeat"
                  defaultMessage="初始化Heartbeat"
                />
              </Button>
            </Popconfirm>
          </FormItem>
          <FormItem {...tailFormItemLayout}>
            <Button type="primary" onClick={this.handleSave}>
              <FormattedMessage
                id="app.components.configCenter.globalConfig.saveConfig"
                defaultMessage="保存配置"
              />
            </Button>
          </FormItem>
        </Form>
      </div>
    )
  }
}

GlobalConfigForm.propTypes = {}
