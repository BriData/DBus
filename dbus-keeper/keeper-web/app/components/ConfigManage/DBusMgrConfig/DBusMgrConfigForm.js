import React, {PropTypes, Component} from 'react'
import {Popconfirm, Form, Select, Input, message, Icon, Button} from 'antd'
import { FormattedMessage } from 'react-intl'
const {TextArea} = Input
const FormItem = Form.Item
const Option = Select.Option

@Form.create()
export default class DBusMgrConfigForm extends Component {
  constructor(props) {
    super(props)
    this.state = {}
  }

  handleSave = () => {
    const {onSave} = this.props
    this.props.form.validateFields((err, values) => {
      if (!err) {
        onSave(values.data)
      }
    })
  }

  handleReset = () => {
    const {onReset} = this.props
    this.props.form.validateFields((err, values) => {
      if (!err) {
        onReset(values.data)
      }
    })
  }

  handleLogin = () => {
    const {onLogin} = this.props
    this.props.form.validateFields((err, values) => {
      if (!err) {
        onLogin(values)
      }
    })
  }

  render() {
    const {getFieldDecorator} = this.props.form
    const loginItemLayout = {
      labelCol: {
        xs: {span: 3},
        sm: {span: 3}
      },
      wrapperCol: {
        xs: {span: 5},
        sm: {span: 5}
      }
    }
    const formItemLayout = {
      labelCol: {
        xs: {span: 3},
        sm: {span: 3}
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
          offset: 3,
        },
        sm: {
          span: 12,
          offset: 3,
        }
      }
    }
    const {data, isLogin} = this.props
    return (
      <div>
          {
            isLogin ? (<Form>
              <FormItem label={<FormattedMessage
                id="app.components.configCenter.mgrConfig.mgrConfig"
                defaultMessage="管理库配置"
              />} {...formItemLayout}>
                {getFieldDecorator('data', {
                  initialValue: data,
                  rules: [
                    {
                      required: true,
                      message: '不能为空'
                    }
                  ]
                })(
                  <TextArea autosize={{minRows: 10}}/>
                )}
              </FormItem>
              <FormItem {...tailFormItemLayout}>
                <Popconfirm title={'该配置很重要，建议不要轻易变动，确定要修改吗？'} onConfirm={this.handleSave} okText="Yes" cancelText="No">
                  <Button type="primary">
                    <FormattedMessage
                      id="app.common.save"
                      defaultMessage="保存"
                    />
                  </Button>
                </Popconfirm>
               {/* <Popconfirm title={'检验整个表数据正确性,drop已经存在的表,并重新执行建表sql！确定重置吗？'} onConfirm={this.handleReset} okText="Yes" cancelText="No">
                  <Button style={{marginLeft: 5}} type="danger">
                    <FormattedMessage
                      id="app.components.configCenter.mgrConfig.reset"
                      defaultMessage="重置"
                    />
                  </Button>
                </Popconfirm>*/}
              </FormItem>
            </Form>) : ( <Form>
              <FormItem label={<FormattedMessage
                id="app.components.configCenter.mgrConfig.adminUser"
                defaultMessage="管理员账号"
              />} {...loginItemLayout}>
                {getFieldDecorator('email', {
                  initialValue: null,
                  rules: [
                    {
                      required: true,
                      message: '不能为空'
                    }
                  ]
                })(
                  <Input
                    type="text"
                  />
                )}
              </FormItem>
              <FormItem label={<FormattedMessage
                id="app.components.configCenter.mgrConfig.adminPassword"
                defaultMessage="管理员密码"
              />} {...loginItemLayout}>
                {getFieldDecorator('password', {
                  initialValue: null,
                  rules: [
                    {
                      required: true,
                      message: '不能为空'
                    }
                  ]
                })(
                  <Input
                    type="password"
                  />
                )}
              </FormItem>
              <FormItem {...tailFormItemLayout}>
                <Button type="primary" onClick={this.handleLogin}>
                  <FormattedMessage
                    id="app.components.configCenter.mgrConfig.login"
                    defaultMessage="确认登录"
                  />
                </Button>
              </FormItem>
            </Form>)
          }


      </div>
    )
  }
}

DBusMgrConfigForm.propTypes = {}
