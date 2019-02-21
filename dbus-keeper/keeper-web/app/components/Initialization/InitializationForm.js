/**
 * @author 戎晓伟
 * @description  登录From 组件
 */

import { FormattedMessage } from 'react-intl'
import React, { PropTypes, Component } from 'react'
import Request, { setToken } from '@/app/utils/request'
import { Form, Input, Button, message, Row, Col } from 'antd'
import { fromJS, is } from 'immutable'
import md5 from 'js-md5'

// 导入样式
import styles from './res/styles/register.less'

const FormItem = Form.Item

@Form.create()
export default class InitializationForm extends Component {

  constructor (props) {
    super(props)
  }

  handleSave = () => {
    this.props.form.validateFieldsAndScroll((err, values) => {
      if (!err) {
        const {onSave} = this.props
        const content = {}
        Object.keys(values).forEach(key => {
          content[key.replace(/@@@/g, '.')] = values[key]
        })
        onSave(content)
      }
    })
  }

  render () {
    const {
      loading,
      kafkaValidateStatus,
      grafanaValidateStatus,
      grafanaTokenValidateStatus,
      influxdbValidateStatus,
      stormValidateStatus,
      stormHomePathValidateStatus,
      stormUIValidateStatus,
      heartbeatValidateStatus,
      heartbeatPathValidateStatus
    } = this.props.state
    const { getFieldDecorator } = this.props.form
    const {basicConf} = this.props
    const formItemLayout = {
      labelCol: {
        xs: { span: 5 },
        sm: { span: 8 }
      },
      wrapperCol: {
        xs: { span: 19 },
        sm: { span: 16 }
      }
    }

    const titleItemLayout = {
      wrapperCol: {
        xs: {offset: 5, span: 19 },
        sm: {offset: 8, span: 16 }
      }
    }
    const contentSpan = 16
    const commentSpan = 7
    const commentOffset = 1
    const commentColor = "#808080"
    return (
      <div>
        <Form autoComplete="off"
          className={styles.register}
          onKeyUp={e => {
            e.keyCode === 13 && this.handleSave()
          }}
        >
          <Row>
            <Col span={contentSpan}>
              <FormItem {...titleItemLayout}>
                <h2>ZK配置信息</h2>
              </FormItem>
            </Col>
          </Row>
          <Row>
            <Col span={contentSpan}>
              <FormItem label="zkServers" {...formItemLayout}>
                {getFieldDecorator('zkServers', {
                  initialValue: basicConf.zkServers,
                  rules: [
                    {
                      required: true,
                      message: 'zkServers不能为空'
                    },
                  ]
                })(<Input disabled placeholder="请输入zkServers" size="large" type="text" />)}
              </FormItem>
            </Col>
          </Row>

          <Row>
            <Col span={contentSpan}>
              <FormItem {...titleItemLayout}>
                <h2>DBus管理库配置信息</h2>
              </FormItem>
            </Col>
          </Row>

          <Row>
            <Col span={contentSpan}>
              <FormItem label="Dbus Manger DB Conn URL" {...formItemLayout}>
                {getFieldDecorator('mgrUrl', {
                  initialValue: basicConf.url,
                  rules: [
                    {
                      required: true,
                      message: 'URL不能为空'
                    }
                  ]
                })(<Input disabled placeholder="请输入URL" size="large" type="text" />)}
              </FormItem>
            </Col>
            <Col style={{color: commentColor}} span={commentSpan} offset={commentOffset}>

            </Col>
          </Row>
          <Row>
            <Col span={contentSpan}>
              <FormItem label="DBus Manger DB Conn User" {...formItemLayout}>
                {getFieldDecorator('DBusUser', {
                  initialValue: basicConf.username,
                  rules: [
                    {
                      required: true,
                      message: 'DBus用户不能为空'
                    }
                  ]
                })(
                  <Input
                    disabled
                    size="large"
                    placeholder="请输入DBusUser"
                  />
                )}
              </FormItem>
            </Col>
            <Col style={{color: commentColor}} span={commentSpan} offset={commentOffset}>

            </Col>
          </Row>
          <Row>
            <Col span={contentSpan}>
              <FormItem {...titleItemLayout}>
                <h2>Kafka配置</h2>
              </FormItem>
            </Col>
          </Row>
          <Row>
            <Col span={contentSpan}>
              <FormItem label="Kafka Bootstrap Servers" hasFeedback validateStatus={kafkaValidateStatus} {...formItemLayout}>
                {getFieldDecorator('bootstrap@@@servers', {
                  initialValue: basicConf["bootstrap.servers"],
                  rules: [
                    {
                      required: true,
                      message: '请输入Kafka的IP和端口'
                    }
                  ]
                })(
                  <Input
                    placeholder="kafka_server1:port1,kafka_server2:port2,kafka_server3:port3"
                    size="large"
                    disabled={true}
                  />
                )}
              </FormItem>
            </Col>
            <Col style={{color: commentColor}} span={commentSpan} offset={commentOffset}>
            </Col>
          </Row>
          <Row>
            <Col span={contentSpan}>
              <FormItem label="Kafka Version" hasFeedback validateStatus={kafkaValidateStatus} {...formItemLayout}>
                {getFieldDecorator('bootstrap@@@servers@@@version', {
                  initialValue: basicConf["bootstrap.servers.version"],
                  rules: [
                    {
                      required: true,
                      message: '请输入Kafka端口'
                    }
                  ]
                })(
                  <Input
                    placeholder="0.10.0.0"
                    size="large"
                    disabled={true}
                  />
                )}
              </FormItem>
            </Col>
            <Col style={{color: commentColor}} span={commentSpan} offset={commentOffset}>
            </Col>
          </Row>
          <Row>
            <Col span={contentSpan}>
              <FormItem {...titleItemLayout}>
                <h2>Influxdb配置</h2>
              </FormItem>
            </Col>
          </Row>

          <Row>
            <Col span={contentSpan}>
              <FormItem label={<FormattedMessage id="app.components.configCenter.globalConfig.influxdbUrl" defaultMessage="Influxdb 外网网址" />} {...formItemLayout}>
                {getFieldDecorator('influxdb_url_web', {
                  initialValue: basicConf['influxdb_url_web'],
                })(<Input disabled={true} type="text" placeholder="请输入Influxdb URL" size="large" />)}
              </FormItem>
            </Col>
            <Col style={{color: commentColor}} span={commentSpan} offset={commentOffset}>
            </Col>
          </Row>
          <Row>
            <Col span={contentSpan}>
              <FormItem label={<FormattedMessage id="app.components.configCenter.globalConfig.influxdbInnerUrl" defaultMessage="Influxdb 内网网址" />} hasFeedback validateStatus={influxdbValidateStatus} {...formItemLayout}>
                {getFieldDecorator('influxdb_url_dbus', {
                  initialValue: basicConf['influxdb_url_dbus'],
                  rules: [
                    {
                      required: true,
                      message: '请输入正确的URL，例如http://influxdb_server_ip:8086'
                    }
                  ]
                })(<Input disabled={true} type="text" placeholder="请输入Influxdb URL" size="large" />)}
              </FormItem>
            </Col>
            <Col style={{color: commentColor}} span={commentSpan} offset={commentOffset}>
            </Col>
          </Row>
          <Row>
            <Col span={contentSpan}>
              <FormItem {...titleItemLayout}>
                <h2>Grafana配置</h2>
              </FormItem>
            </Col>
          </Row>
          <Row>
            <Col span={contentSpan}>
              <FormItem label={<FormattedMessage id="app.components.configCenter.globalConfig.grafanaUrl" defaultMessage="Grafana 外网网址" />} {...formItemLayout}>
                {getFieldDecorator('grafana_url_web', {
                  initialValue: 'http://grafana_server_ip:3000',
                })(<Input type="text" placeholder="Grafana URL" size="large" />)}
              </FormItem>
            </Col>
            <Col style={{color: commentColor}} span={commentSpan} offset={commentOffset}>
              例如：http://grafana_server_ip:3000
            </Col>
          </Row>
          <Row>
            <Col span={contentSpan}>
              <FormItem label={<FormattedMessage id="app.components.configCenter.globalConfig.grafanaInnerUrl" defaultMessage="Grafana 内网网址" />}  hasFeedback validateStatus={grafanaValidateStatus} {...formItemLayout}>
                {getFieldDecorator('grafana_url_dbus', {
                  initialValue: 'http://grafana_server_ip:3000',
                  rules: [
                    {
                      required: true,
                      message: 'Please Input Grafana URL'
                    }
                  ]
                })(<Input type="text" placeholder="Grafana URL" size="large" />)}
              </FormItem>
            </Col>
            <Col style={{color: commentColor}} span={commentSpan} offset={commentOffset}>
              例如：http://grafana_server_ip:3000
            </Col>
          </Row>
          <Row>
            <Col span={contentSpan}>
              <FormItem label="Grafana Token"  hasFeedback validateStatus={grafanaTokenValidateStatus} {...formItemLayout}>
                {getFieldDecorator('grafanaToken', {
                  initialValue: 'Your Token',
                  rules: [
                    {
                      required: true,
                      message: '请输入Grafana Token'
                    }
                  ]
                })(<Input type="text" placeholder="请输入Grafana Token" size="large" />)}
              </FormItem>
            </Col>
            <Col style={{color: commentColor}} span={commentSpan} offset={commentOffset}>
              通过Grafana生成的API Key
            </Col>
          </Row>
          <Row>
            <Col span={contentSpan}>
              <FormItem {...titleItemLayout}>
                <h2>Storm配置</h2>
              </FormItem>
            </Col>
          </Row>
          <Row>
            <Col span={contentSpan}>
              <FormItem label="Storm Nimbus 主机"  hasFeedback validateStatus={stormValidateStatus} {...formItemLayout}>
                {getFieldDecorator('storm@@@nimbus@@@host', {
                  initialValue: 'storm_nimbus_ip',
                  rules: [
                    {
                      required: true,
                      message: '请输入Storm Nimbus 主机'
                    }
                  ]
                })(<Input type="text" placeholder="请输入Storm Nimbus 主机" size="large" />)}
              </FormItem>
            </Col>
            <Col style={{color: commentColor}} span={commentSpan} offset={commentOffset}>
              Storm Nimbus所在机器的ip地址，例如：dbus-n1
            </Col>
          </Row>
          <Row>
            <Col span={contentSpan}>
              <FormItem label="Storm Nimbus 主机 SSH 端口号"  hasFeedback validateStatus={stormValidateStatus} {...formItemLayout}>
                {getFieldDecorator('storm@@@nimbus@@@port', {
                  initialValue: '22',
                  rules: [
                    {
                      required: true,
                      message: '请输入Storm Nimbus 端口'
                    },
                    {
                      pattern: /^\d{1,5}$/,
                      message: '请输入正确端口'
                    }
                  ]
                })(<Input type="text" placeholder="请输入Storm Nimbus 端口" size="large" />)}
              </FormItem>
            </Col>
            <Col style={{color: commentColor}} span={commentSpan} offset={commentOffset}>
              Storm Nimbus所在机器的SSH服务端口号，通常为22
            </Col>
          </Row>
          <Row>
            <Col span={contentSpan}>
              <FormItem label="免密登录启动Storm用户"  hasFeedback validateStatus={stormValidateStatus} {...formItemLayout}>
                {getFieldDecorator('user', {
                  initialValue: 'app',
                  rules: [
                    {
                      required: true,
                      message: '请输入Storm用户'
                    }
                  ]
                })(<Input type="text" placeholder="请输入Storm用户" size="large" />)}
              </FormItem>
            </Col>
            <Col style={{color: commentColor}} span={commentSpan} offset={commentOffset}>
              Storm Nimbus所在机器的SSH服务用户名
            </Col>
          </Row>
          <Row>
            <Col span={contentSpan}>
              <FormItem label="Storm Home 路径" hasFeedback validateStatus={stormHomePathValidateStatus} {...formItemLayout}>
                {getFieldDecorator('storm@@@home@@@path', {
                  initialValue: '/app/dbus/apache-storm-1.0.2/',
                  rules: [
                    {
                      required: true,
                      message: '请输入路径'
                    }
                  ]
                })(<Input type="text" placeholder="请输入路径" size="large" />)}
              </FormItem>
            </Col>
            <Col style={{color: commentColor}} span={commentSpan} offset={commentOffset}>
              Storm Nimbus所在机器的Storm根目录
            </Col>
          </Row>

          <Row>
            <Col span={contentSpan}>
              <FormItem label="Storm UI 地址" hasFeedback validateStatus={stormUIValidateStatus} {...formItemLayout}>
                {getFieldDecorator('stormRest', {
                  initialValue: 'http://storm_ui_ip:8080/api/v1',
                  rules: [
                    {
                      required: true,
                      message: '请输入Storm Rest API URL'
                    }
                  ]
                })(<Input type="text" placeholder="请输入Storm Rest API 地址" size="large" />)}
              </FormItem>
            </Col>
            <Col style={{color: commentColor}} span={commentSpan} offset={commentOffset}>
              Storm UI接口地址，例如：http://dbus-n1:8080/api/v1
            </Col>
          </Row>
          <Row>
            <Col span={contentSpan}>
              <FormItem {...titleItemLayout}>
                <h2>心跳自动部署配置</h2>
              </FormItem>
            </Col>
          </Row>
          <Row>
            <Col span={contentSpan}>
              <FormItem label="心跳服务器IP" hasFeedback validateStatus={heartbeatValidateStatus} {...formItemLayout}>
                {getFieldDecorator('heartbeat@@@host', {
                  initialValue: 'dbus-heartbeat-ip1,dbus-heartbeat-ip2',
                  rules: [
                    {
                      required: true,
                      message: '请输入心跳服务器IP'
                    }
                  ]
                })(<Input type="text" placeholder="请输入心跳服务器IP" size="large" />)}
              </FormItem>
            </Col>
            <Col style={{color: commentColor}} span={commentSpan} offset={commentOffset}>
              例如：dbus-n1,dbus-n2,dbus-n3
            </Col>
          </Row>
          <Row>
            <Col span={contentSpan}>
              <FormItem label="心跳服务器SSH端口" hasFeedback validateStatus={heartbeatValidateStatus} {...formItemLayout}>
                {getFieldDecorator('heartbeat@@@port', {
                  initialValue: '22',
                  rules: [
                    {
                      required: true,
                      message: '请输入心跳服务器SSH端口'
                    },
                    {
                      pattern: /^\d{1,5}$/,
                      message: '请输入正确的端口'
                    }
                  ]
                })(<Input type="text" placeholder="请输入心跳服务器SSH端口" size="large" />)}
              </FormItem>
            </Col>
            <Col style={{color: commentColor}} span={commentSpan} offset={commentOffset}>
              通常SSH服务的端口为22
            </Col>
          </Row>
          <Row>
            <Col span={contentSpan}>
              <FormItem label="心跳服务器SSH用户名" hasFeedback validateStatus={heartbeatValidateStatus} {...formItemLayout}>
                {getFieldDecorator('heartbeat@@@user', {
                  initialValue: 'app',
                  rules: [
                    {
                      required: true,
                      message: '请输入心跳服务器SSH用户名'
                    }
                  ]
                })(<Input type="text" placeholder="请输入心跳服务器SSH用户名" size="large" />)}
              </FormItem>
            </Col>
            <Col style={{color: commentColor}} span={commentSpan} offset={commentOffset}>
              SSH服务的用户名
            </Col>
          </Row>
          <Row>
            <Col span={contentSpan}>
              <FormItem label="心跳安装路径" hasFeedback validateStatus={heartbeatPathValidateStatus} {...formItemLayout}>
                {getFieldDecorator('heartbeat@@@jar@@@path', {
                  initialValue: '/app/dbus/heartbeat',
                  rules: [
                    {
                      required: true,
                      message: '请输入心跳安装路径'
                    }
                  ]
                })(<Input type="text" placeholder="请输入心跳安装路径" size="large" />)}
              </FormItem>
            </Col>
            <Col style={{color: commentColor}} span={commentSpan} offset={commentOffset}>
              安装位置，默认即可
            </Col>
          </Row>
          <Row>
            <Col span={contentSpan}>
              <FormItem {...titleItemLayout}>
                <h2>心跳报警配置</h2>
              </FormItem>
            </Col>
            <Col style={{color: commentColor}} span={commentSpan} offset={commentOffset}>

            </Col>
          </Row>
          <Row>
            <Col span={contentSpan}>
              <FormItem label="报警邮件SMTP地址" {...formItemLayout}>
                {getFieldDecorator('alarmMailSMTPAddress', {
                  initialValue: 'smtp.163.com'
                })(<Input type="text" placeholder="请输入SMTP地址" size="large" />)}
              </FormItem>
            </Col>
            <Col style={{color: commentColor}} span={commentSpan} offset={commentOffset}>

            </Col>
          </Row>
          <Row>
            <Col span={contentSpan}>
              <FormItem label="报警邮件SMTP端口" {...formItemLayout}>
                {getFieldDecorator('alarmMailSMTPPort', {
                  initialValue: '25'
                })(<Input type="text" placeholder="请输入SMTP端口" size="large" />)}
              </FormItem>
            </Col>
            <Col style={{color: commentColor}} span={commentSpan} offset={commentOffset}>

            </Col>
          </Row>
          <Row>
            <Col span={contentSpan}>
              <FormItem label="报警邮件发送邮箱" {...formItemLayout}>
                {getFieldDecorator('alarmSendEmail', {
                  initialValue: 'dbus_admin@dbus.com'
                })(<Input type="text" placeholder="请输入Email" size="large" />)}
              </FormItem>
            </Col>
            <Col style={{color: commentColor}} span={commentSpan} offset={commentOffset}>

            </Col>
          </Row>

          <Row>
            <Col span={contentSpan}>
              <FormItem label="报警邮箱用户名" {...formItemLayout}>
                {getFieldDecorator('alarmMailUser', {
                  initialValue: 'superuser'
                })(<Input type="text" placeholder="请输入用户名" size="large" />)}
              </FormItem>
            </Col>
            <Col style={{color: commentColor}} span={commentSpan} offset={commentOffset}>

            </Col>
          </Row>
          <Row>
            <Col span={contentSpan}>
              <FormItem label="报警邮箱密码" {...formItemLayout}>
                {getFieldDecorator('alarmMailPass', {
                  initialValue: '12345678'
                })(<Input type="text" placeholder="请输入密码" size="large" />)}
              </FormItem>
            </Col>
            <Col style={{color: commentColor}} span={commentSpan} offset={commentOffset}>

            </Col>
          </Row>
          <Row>
            <Col span={contentSpan}>
              <FormItem
                wrapperCol={{
                  sm: {
                    span: 16,
                    offset: 8
                  }
                }}
              >
                <Button
                  type="primary"
                  size="large"
                  onClick={this.handleSave}
                  className={styles.submit}
                  loading={loading}
                >
                  确定
                </Button>
              </FormItem>
            </Col>
          </Row>
        </Form>
      </div>
    )
  }
}

InitializationForm.propTypes = {
  form: PropTypes.any,
  loginApi: PropTypes.string,
  registerApi: PropTypes.string
}
