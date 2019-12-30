import React, {Component} from 'react'
import {Button, Checkbox, Col, Form, Input, Row, Select} from 'antd'
import {FormattedMessage} from 'react-intl'
// 导入样式
const Textarea = Input.TextArea

const FormItem = Form.Item
const Option = Select.Option

@Form.create()
export default class BasicConfigForm extends Component {
  constructor(props) {
    super(props)
    this.state = {}
  }


  render() {
    const {getFieldDecorator} = this.props.form
    const formItemLayout = {
      labelCol: {
        span: 5
      },
      wrapperCol: {
        span: 19
      }
    }
    const tailFormItemLayout = {
      wrapperCol: {
        span: 19,
        offset: 5,
      }
    }
    const {config, onValueChange, onCheckboxChange, onSave, onSendMailTest} = this.props
    return (
      <div>
        <Form>
          <Row>
            <Col span={15}>
              <FormItem label={'报警邮件收件箱'} {...formItemLayout}>
                {getFieldDecorator('adminEmail', {
                  initialValue: config['adminEmail'],
                  rules: [
                    {
                      required: true,
                      message: '不能为空'
                    }
                  ]
                })(
                  <Input onChange={e => onValueChange(e.target.value, 'adminEmail')} placeholder="admin@xxx.com"
                         size="large"
                         type="text"/>
                )}
              </FormItem>
            </Col>
            <Col span={7}>
              <FormItem label={<FormattedMessage
                id="app.components.configCenter.heartbeatConfig.enable"
                defaultMessage="启用"
              />} {...formItemLayout}>
                {getFieldDecorator('adminUseEmail', {
                  initialValue: config['adminUseEmail'] === 'Y',
                  rules: [
                    {
                      required: true,
                      message: '不能为空'
                    }
                  ],
                  valuePropName: 'checked'
                })(
                  <Checkbox onChange={e => onCheckboxChange(e.target.checked, 'adminUseEmail')}/>
                )}
              </FormItem>
            </Col>
          </Row>
          <Row>
            <Col span={15}>
              <FormItem label={'报警短信接收手机号'} {...formItemLayout}>
                {getFieldDecorator('adminSMSNo', {
                  initialValue: config['adminSMSNo'],
                  rules: [
                    {
                      required: true,
                      message: '不能为空'
                    }
                  ]
                })(
                  <Input onChange={e => onValueChange(e.target.value, 'adminSMSNo')} placeholder="13000000000"
                         size="large" type="text"/>
                )}
              </FormItem>
            </Col>
            <Col span={7}>
              <FormItem label={<FormattedMessage
                id="app.components.configCenter.heartbeatConfig.enable"
                defaultMessage="启用"
              />
              } {...formItemLayout}>
                {getFieldDecorator('adminUseSMS', {
                  initialValue: config['adminUseSMS'] === 'Y',
                  rules: [
                    {
                      required: true,
                      message: '不能为空'
                    }
                  ],
                  valuePropName: 'checked'
                })(
                  <Checkbox onChange={e => onCheckboxChange(e.target.checked, 'adminUseSMS')}/>
                )}
              </FormItem>
            </Col>
          </Row>

          <Row>
            <Col span={15}>
              <FormItem label={'表结构变更通知邮箱'} {...formItemLayout}>
                {getFieldDecorator('schemaChangeEmail', {
                  initialValue: config['schemaChangeEmail'],
                  rules: [
                    {
                      required: true,
                      message: '不能为空'
                    }
                  ]
                })(
                  <Input onChange={e => onValueChange(e.target.value, 'schemaChangeEmail')} placeholder="admin@xxx.com"
                         size="large" type="text"/>
                )}
              </FormItem>
            </Col>
            <Col span={7}>
              <FormItem label={<FormattedMessage
                id="app.components.configCenter.heartbeatConfig.enable"
                defaultMessage="启用"
              />} {...formItemLayout}>
                {getFieldDecorator('schemaChangeUseEmail', {
                  initialValue: config['schemaChangeUseEmail'] === 'Y',
                  rules: [
                    {
                      required: true,
                      message: '不能为空'
                    }
                  ],
                  valuePropName: 'checked'
                })(
                  <Checkbox onChange={e => onCheckboxChange(e.target.checked, 'schemaChangeUseEmail')}/>
                )}
              </FormItem>
            </Col>
          </Row>

          <Row>
            <Col span={15}>
              <FormItem label={'报警邮箱名'} {...formItemLayout}>
                {getFieldDecorator('alarmSendEmail', {
                  initialValue: config['alarmSendEmail'],
                  rules: [
                    {
                      required: true,
                      message: '不能为空'
                    }
                  ]
                })(
                  <Input onChange={e => onValueChange(e.target.value, 'alarmSendEmail')} placeholder="alarm@xxc.com"
                         size="large" type="text"/>
                )}
              </FormItem>
            </Col>
          </Row>
          <Row>
            <Col span={15}>
              <FormItem label={'报警邮箱服务地址'} {...formItemLayout}>
                {getFieldDecorator('alarmMailSMTPAddress', {
                  initialValue: config['alarmMailSMTPAddress'],
                  rules: [
                    {
                      required: true,
                      message: '不能为空'
                    }
                  ]
                })(
                  <Input onChange={e => onValueChange(e.target.value, 'alarmMailSMTPAddress')}
                         placeholder="smtp.xxx.com"
                         size="large" type="text"/>
                )}
              </FormItem>
            </Col>
          </Row>
          <Row>
            <Col span={15}>
              <FormItem label={'报警邮箱服务端口号'} {...formItemLayout}>
                {getFieldDecorator('alarmMailSMTPPort', {
                  initialValue: config['alarmMailSMTPPort'],
                  rules: [
                    {
                      required: true,
                      message: '不能为空'
                    }
                  ]
                })(
                  <Input onChange={e => onValueChange(e.target.value, 'alarmMailSMTPPort')} placeholder="994"
                         size="large" type="text"/>
                )}
              </FormItem>
            </Col>
          </Row>
          <Row>
            <Col span={15}>
              <FormItem label={'报警邮件发件人'} {...formItemLayout}>
                {getFieldDecorator('alarmMailUser', {
                  initialValue: config['alarmMailUser'],
                  rules: [
                    {
                      required: true,
                      message: '不能为空'
                    }
                  ]
                })(
                  <Input onChange={e => onValueChange(e.target.value, 'alarmMailUser')} placeholder="alarm@xxx.com"
                         size="large" type="text"/>
                )}
              </FormItem>
            </Col>
          </Row>
          <Row>
            <Col span={15}>
              <FormItem label={'报警邮件发件人密码'} {...formItemLayout}>
                {getFieldDecorator('alarmMailPass', {
                  initialValue: config['alarmMailPass'],
                  rules: [
                    {
                      required: true,
                      message: '不能为空'
                    }
                  ]
                })(
                  <Input onChange={e => onValueChange(e.target.value, 'alarmMailPass')} placeholder="password"
                         size="large" type="text"/>
                )}
              </FormItem>
            </Col>
          </Row>
          <Row>
            <Col span={15}>
              <FormItem {...tailFormItemLayout}>
                <Button type="primary" onClick={onSendMailTest}>
                  邮件测试
                </Button>
              </FormItem>
            </Col>
            <Col span={15}>
              <FormItem {...tailFormItemLayout}>
                <Button type="primary" onClick={onSave}>
                  <FormattedMessage
                    id="app.common.save"
                    defaultMessage="保存"
                  />
                </Button>
              </FormItem>
            </Col>
          </Row>

        </Form>
      </div>
    )
  }
}

BasicConfigForm.propTypes = {}
