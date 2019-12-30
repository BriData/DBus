import React, {Component} from 'react'
import {Button, Form, Input, Popconfirm, Popover} from 'antd'
import {FormattedMessage} from 'react-intl'

const FormItem = Form.Item

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
        const {config} = this.props
        onSave({...config, ...content})
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
        const {config} = this.props
        onInit(options, {...config, ...content})
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
          <FormItem label={<FormattedMessage id="app.components.configCenter.globalConfig.dbus.cluster.server.list"
                                             defaultMessage="dbus集群服务器列表"/>} {...formItemLayout}>
            {getFieldDecorator('dbus@@@cluster@@@server@@@list', {
              initialValue: config['dbus.cluster.server.list'],
              rules: [
                {
                  required: true,
                  message: '不能为空'
                }
              ]
            })(
              <Input placeholder="请输入dbus集群服务器列表" size="large" type="text"/>
            )}
          </FormItem>
          <FormItem label={<FormattedMessage id="app.components.configCenter.globalConfig.dbus.cluster.ssh.port"
                                             defaultMessage="dbus集群免密端口号"/>} {...formItemLayout}>
            {getFieldDecorator('dbus@@@cluster@@@server@@@ssh@@@port', {
              initialValue: config['dbus.cluster.server.ssh.port'],
              rules: [
                {
                  required: true,
                  message: '不能为空'
                }
              ]
            })(
              <Input placeholder="请输入dbus集群免密端口号" size="large" type="text"/>
            )}
          </FormItem>
          <FormItem label={<FormattedMessage id="app.components.configCenter.globalConfig.dbus.cluster.ssh.user"
                                             defaultMessage="dbus集群免密用户名"/>} {...formItemLayout}>
            {getFieldDecorator('dbus@@@cluster@@@server@@@ssh@@@user', {
              initialValue: config['dbus.cluster.server.ssh.user'],
              rules: [
                {
                  required: true,
                  message: '不能为空'
                }
              ]
            })(
              <Input placeholder="请输入dbus集群免密用户名" size="large" type="text"/>
            )}
          </FormItem>
          <FormItem label={<FormattedMessage
            id="app.components.configCenter.globalConfig.zkServers"
            defaultMessage="ZK集群地址"
          />} {...formItemLayout}>
            {getFieldDecorator('zk@@@str', {
              initialValue: config['zk.str'],
              rules: [
                {
                  required: true,
                  message: '不能为空'
                }
              ]
            })(
              <Input placeholder="请输入ZK集群地址" size="large" type="text"/>
            )}
          </FormItem>
          <FormItem {...tailFormItemLayout}>
            <Popconfirm title={'确认初始化ZK节点？'} onConfirm={this.handleInitZk} okText="Yes" cancelText="No">
              <Popover content={'根据/DBus/ConfTemplates模板节点,初始化zk其他dbus组件节点,已存在的节点不作处理！'} title="初始化ZK节点"
                       trigger="hover">
                <Button type="danger">
                  <FormattedMessage
                    id="app.components.configCenter.globalConfig.initZk"
                    defaultMessage="保存全局配置并初始化ZK节点"
                  />
                </Button>
              </Popover>
            </Popconfirm>
          </FormItem>
          <FormItem label={<FormattedMessage id="app.components.configCenter.globalConfig.bootstrapServers"
                                             defaultMessage="Kafka集群地址"
          />} {...formItemLayout}>
            {getFieldDecorator('bootstrap@@@servers', {
              initialValue: config['bootstrap.servers'],
              rules: [
                {
                  required: true,
                  message: '不能为空'
                }
              ]
            })(
              <Input placeholder="请输入Kafka集群地址" size="large" type="text"/>
            )}
          </FormItem>
          <FormItem label={<FormattedMessage id="app.components.configCenter.globalConfig.bootstrapServersVersion"
                                             defaultMessage="Kafka版本"/>} {...formItemLayout}>
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
          <FormItem label={<FormattedMessage id="app.components.configCenter.globalConfig.grafanaUrl"
                                             defaultMessage="Grafana外网网址"/>} {...formItemLayout}>
            {getFieldDecorator('grafana@@@web@@@url', {
              initialValue: config['grafana.web.url'],
              rules: [
                {
                  required: true,
                  message: '不能为空'
                }
              ]
            })(
              <Input placeholder="请输入Grafana外网网址" size="large" type="text"/>
            )}
          </FormItem>
          <FormItem label={<FormattedMessage id="app.components.configCenter.globalConfig.grafanaInnerUrl"
                                             defaultMessage="Grafana内网网址"/>} {...formItemLayout}>
            {getFieldDecorator('grafana@@@dbus@@@url', {
              initialValue: config['grafana.dbus.url'],
              rules: [
                {
                  required: true,
                  message: '不能为空'
                }
              ]
            })(
              <Input placeholder="请输入Grafana内网网址" size="large" type="text"/>
            )}
          </FormItem>
          <FormItem label='Grafana Token' {...formItemLayout}>
            {getFieldDecorator('grafana@@@token', {
              initialValue: config['grafana.token'],
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
              <Popover content={'新建data source,导入Grafana Dashboard模板！'} title="初始化Grafana" trigger="hover">
                <Button type="danger">
                  <FormattedMessage
                    id="app.components.configCenter.globalConfig.initGrafana"
                    defaultMessage="保存全局配置并初始化Grafana"
                  />
                </Button>
              </Popover>
            </Popconfirm>
          </FormItem>
          <FormItem label={<FormattedMessage id="app.components.configCenter.globalConfig.influxdbUrl"
                                             defaultMessage="Influxdb外网网址"/>} {...formItemLayout}>
            {getFieldDecorator('influxdb@@@web@@@url', {
              initialValue: config['influxdb.web.url'],
              rules: [
                {
                  required: true,
                  message: '不能为空'
                }
              ]
            })(
              <Input placeholder="请输入Influxdb外网网址" size="large" type="text"/>
            )}
          </FormItem>
          <FormItem label={<FormattedMessage id="app.components.configCenter.globalConfig.influxdbInnerUrl"
                                             defaultMessage="Influxdb内网网址"/>} {...formItemLayout}>
            {getFieldDecorator('influxdb@@@dbus@@@url', {
              initialValue: config['influxdb.dbus.url'],
              rules: [
                {
                  required: true,
                  message: '不能为空'
                }
              ]
            })(
              <Input placeholder="请输入Influxdb内网网址" size="large" type="text"/>
            )}
          </FormItem>
          <FormItem {...tailFormItemLayout}>
            <Popconfirm title={'确认初始化Influxdb？'} onConfirm={this.handleInitInfluxdb} okText="Yes" cancelText="No">
              <Popover content={'新建数据库和用户,设置数据保留策略15天！'} title="初始化Influxdb" trigger="hover">
                <Button type="danger">
                  <FormattedMessage
                    id="app.components.configCenter.globalConfig.initInfluxdb"
                    defaultMessage="保存全局配置并初始化Influxdb"
                  />
                </Button>
              </Popover>
            </Popconfirm>
          </FormItem>
          <FormItem label={<FormattedMessage id="app.components.configCenter.globalConfig.stormNimbusHost"
                                             defaultMessage="Storm Nimbus服务器地址"/>} {...formItemLayout}>
            {getFieldDecorator('storm@@@nimbus@@@host', {
              initialValue: config['storm.nimbus.host'],
              rules: [
                {
                  required: true,
                  message: '不能为空'
                }
              ]
            })(
              <Input placeholder="请输入Storm Nimbus服务器地址" size="large" type="text"/>
            )}
          </FormItem>
          <FormItem label={<FormattedMessage
            id="app.components.configCenter.globalConfig.stormHomePath"
            defaultMessage="Storm Nimbus根目录"
          />} {...formItemLayout}>
            {getFieldDecorator('storm@@@nimbus@@@home@@@path', {
              initialValue: config['storm.nimbus.home.path'],
              rules: [
                {
                  required: true,
                  message: '不能为空'
                }
              ]
            })(
              <Input placeholder="请输入Storm Nimbus根目录" size="large" type="text"/>
            )}
          </FormItem>
          <FormItem label={<FormattedMessage
            id="app.components.configCenter.globalConfig.stormLogPath"
            defaultMessage="Storm日志根目录"
          />} {...formItemLayout}>
            {getFieldDecorator('storm@@@nimbus@@@log@@@path', {
              initialValue: config['storm.nimbus.log.path'],
              rules: [
                {
                  required: true,
                  message: '不能为空'
                }
              ]
            })(
              <Input placeholder="请输入Storm日志根目录" size="large" type="text"/>
            )}
          </FormItem>
          <FormItem label={<FormattedMessage
            id="app.components.configCenter.globalConfig.stormUIRestApi"
            defaultMessage="Storm UI接口地址"
          />} {...formItemLayout}>
            {getFieldDecorator('storm@@@rest@@@url', {
              initialValue: config['storm.rest.url'],
              rules: [
                {
                  required: true,
                  message: '不能为空'
                }
              ]
            })(
              <Input placeholder="请输入Storm UI接口地址" size="large" type="text"/>
            )}
          </FormItem>
          <FormItem {...tailFormItemLayout}>
            <Popconfirm title={'确认初始化Storm？'} onConfirm={this.handleInitStorm} okText="Yes" cancelText="No">
              <Popover content={'如果之前初始化过,删除并重新上传jar包和脱敏包！'} title="确认初始化Storm"
                       trigger="hover">
                <Button type="danger">
                  <FormattedMessage
                    id="app.components.configCenter.globalConfig.initStorm"
                    defaultMessage="保存全局配置并初始化Storm"
                  />
                </Button>
              </Popover>
            </Popconfirm>
          </FormItem>
          <FormItem label={<FormattedMessage
            id="app.components.configCenter.globalConfig.heartbeatIP"
            defaultMessage="心跳服务器地址"
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
              <Input placeholder="请输入心跳服务器地址" size="large" type="text"/>
            )}
          </FormItem>
          <FormItem label={<FormattedMessage
            id="app.components.configCenter.globalConfig.heartbeatPath"
            defaultMessage="心跳服务根目录"
          />} {...formItemLayout}>
            {getFieldDecorator('heartbeat@@@path', {
              initialValue: config['heartbeat.path'],
              rules: [
                {
                  required: true,
                  message: '不能为空'
                }
              ]
            })(
              <Input placeholder="请输入心跳服务根目录" size="large" type="text"/>
            )}
          </FormItem>
          <FormItem {...tailFormItemLayout}>
            <Popconfirm title={'确认初始化心跳？'}
                        onConfirm={this.handleInitHeartbeat} okText="Yes" cancelText="No">
              <Popover content={'如果之前初始化过,删除并重新上传心跳包,重新启动心跳程序!'} title="确认初始化心跳" trigger="hover">
                <Button type="danger">
                  <FormattedMessage
                    id="app.components.configCenter.globalConfig.initHeartbeat"
                    defaultMessage="保存全局配置并初始化Heartbeat"
                  />
                </Button>
              </Popover>
            </Popconfirm>
          </FormItem>
          <FormItem {...tailFormItemLayout}>
            <Popconfirm title={'确认保存全局配置？'} onConfirm={this.handleSave} okText="Yes" cancelText="No">
              <Button type="primary">
                <FormattedMessage
                  id="app.components.configCenter.globalConfig.saveConfig"
                  defaultMessage="保存全局配置"
                />
              </Button>
            </Popconfirm>
          </FormItem>
        </Form>
      </div>
    )
  }
}

GlobalConfigForm.propTypes = {}
