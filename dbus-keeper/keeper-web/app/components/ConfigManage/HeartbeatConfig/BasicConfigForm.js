import React, {PropTypes, Component} from 'react'
import {Form, Select, Input, Button, Checkbox, Row, Col} from 'antd'
const Textarea = Input.TextArea
// 导入样式
import styles from './res/styles/index.less'
import Request from "@/app/utils/request";

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
    const {config, onValueChange, onCheckboxChange, onSave} = this.props
    return (
      <div>
        <Form>
          <Row>
            <Col span={15}>
              <FormItem label='Admin邮箱' {...formItemLayout}>
                {getFieldDecorator('adminEmail', {
                  initialValue: config['adminEmail'],
                  rules: [
                    {
                      required: true,
                      message: '不能为空'
                    }
                  ]
                })(
                  <Input onChange={e => onValueChange(e.target.value, 'adminEmail')} placeholder="Admin邮箱" size="large" type="text"/>
                )}
              </FormItem>
            </Col>
            <Col span={6}>
              <FormItem label='启用' {...formItemLayout}>
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
              <FormItem label='Admin短信手机号' {...formItemLayout}>
                {getFieldDecorator('adminSMSNo', {
                  initialValue: config['adminSMSNo'],
                  rules: [
                    {
                      required: true,
                      message: '不能为空'
                    }
                  ]
                })(
                  <Input onChange={e => onValueChange(e.target.value, 'adminSMSNo')} placeholder="Admin短信手机号" size="large" type="text"/>
                )}
              </FormItem>
            </Col>
            <Col span={6}>
              <FormItem label='启用' {...formItemLayout}>
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
              <FormItem label='Schema变更通知邮箱' {...formItemLayout}>
                {getFieldDecorator('schemaChangeEmail', {
                  initialValue: config['schemaChangeEmail'],
                  rules: [
                    {
                      required: true,
                      message: '不能为空'
                    }
                  ]
                })(
                    <Input onChange={e => onValueChange(e.target.value, 'schemaChangeEmail')} placeholder="Schema变更通知邮箱" size="large" type="text"/>
                )}
              </FormItem>
            </Col>
            <Col span={6}>
              <FormItem label='启用' {...formItemLayout}>
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
            <Col span={12}>
              <FormItem {...tailFormItemLayout}>
                <Button type="primary" onClick={onSave}>保存</Button>
              </FormItem>
            </Col>
          </Row>

        </Form>
      </div>
    )
  }
}

BasicConfigForm.propTypes = {}
