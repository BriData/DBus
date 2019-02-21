/**
 * @author xiancangao
 * @description  基本信息设置
 */
import React, { PropTypes, Component } from 'react'
import {Form, Input, Button, Select, message } from 'antd'
import JSONTree from 'react-json-tree'
import dateFormat from 'dateformat'
import { FormattedMessage } from 'react-intl'
import {
  BATCH_SEND_CONTROL_MESSAGE_API
} from '@/app/containers/toolSet/api'
import Request from "@/app/utils/request";
const Option = Select.Option
const FormItem = Form.Item
@Form.create({ warppedComponentRef: true })
export default class BatchControlMessageForm extends Component {
  constructor(props) {
    super(props)
  }

  handleSend = () => {
    const {validateFields}=this.props.form
    validateFields((error,values)=>{
       if(!error){
         //提交
         Request(BATCH_SEND_CONTROL_MESSAGE_API, {
          params: {
            reloadType: values.messageType
          },
          method: 'get'
        })
          .then(res => {
            if (res && res.status === 0) {
              message.success(res.message)
            } else {
              message.warn(res.message)
            }
          })
          .catch(error => {
            error.response&&error.response.data && error.response.data.message
              ? message.error(error.response.data.message)
              : message.error(error.message)
          })
       }
    })
  }

  render() {
    const { getFieldDecorator, getFieldsValue } = this.props.form
    const { messageTypeGroup } = this.props
    const { messageType } = getFieldsValue()
    const formItemLayout = {
      labelCol: { span: 4 },
      wrapperCol: { span: 10 }
    }

    const theme = {
      scheme: 'monokai',
      base00: '#272822'
    }
    return (
      <div className="form-search">

        <Form autoComplete="off" layout="horizontal">
          <FormItem
            label={<FormattedMessage
              id="app.components.toolset.controlMessage.messageType"
              defaultMessage="消息类型"
            />}
            {...formItemLayout}
          >
            {getFieldDecorator('messageType', {
              rules: [
                {
                  required: true,
                  message: '请选择消息类型'
                }
              ]
            })(
              <Select
                style={{width:"500px"}}
                showSearch
                placeholder="Select message type"
                optionFilterProp="children"
                filterOption={(input, option) => option.props.children.toLowerCase().indexOf(input.toLowerCase()) >= 0}
              >
                {messageTypeGroup.map(item => (
                  <Option value={item.value} key={item.value}>
                    {item.text}
                  </Option>
                ))}
              </Select>
            )}
          </FormItem>
          <FormItem
            wrapperCol={{
              xs: { span: 24, offset: 0 },
              sm: { span: 16, offset: 4 }
            }}
          >
            <Button onClick={this.handleSend} type="primary" htmlType="submit">
              <FormattedMessage
                id="app.components.toolset.controlMessage.sendControlMessage"
                defaultMessage="发送控制消息"
              />
            </Button>
          </FormItem>
        </Form>
      </div>
    )
  }
}

BatchControlMessageForm.propTypes = {
  form: PropTypes.object,
  messageTypeChanged: PropTypes.func
}
