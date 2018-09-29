/**
 * @author xiancangao
 * @description  基本信息设置
 */
import React, {PropTypes, Component} from 'react'
import {Form, message, Input, Button, Select} from 'antd'
import JSONTree from 'react-json-tree'
import { FormattedMessage } from 'react-intl'
import dateFormat from 'dateformat'
import {KAFKA_READER_GET_OFFSET_RANGE_API} from '@/app/containers/toolSet/api'
import Request from "@/app/utils/request";
const TextArea = Input.TextArea
const Option = Select.Option
const FormItem = Form.Item
@Form.create({warppedComponentRef: true})
export default class KafkaReaderForm extends Component {
  constructor (props) {
    super(props)
    this.state = {
      beginOffset: 0,
      endOffset: 0,
      maxReadCount: 0
    }
  }

  handleRead = () => {
    this.props.form.validateFields((err, values) => {
      if (!err) {
        const {onRead} = this.props
        onRead(values)
      }
    })
  }

  handleTopicChange = topic => {
    const {topicList} = this.props
    if (!topicList.some(t => t === topic)) {
      return
    }
    Request(KAFKA_READER_GET_OFFSET_RANGE_API, {
      params: {topic},
      method: 'get' })
      .then(res => {
        if (res && res.status === 0) {
          const {beginOffset, endOffset} = res.payload
          this.setState({
            beginOffset,
            endOffset,
            maxReadCount: endOffset - beginOffset
          })
          this.props.form.setFieldsValue({
            from: beginOffset,
            length: Math.min(20, endOffset - beginOffset)
          })
        } else {
          message.warn(res.message)
        }
      })
      .catch(error => {
        error.response.data && error.response.data.message
          ? message.error(error.response.data.message)
          : message.error(error.message)
      })
  }

  render () {
    const {getFieldDecorator} = this.props.form
    const formItemLayout = {
      labelCol: {span: 4},
      wrapperCol: {span: 10}
    }
    const {topicList, kafkaData} = this.props
    const {beginOffset, endOffset, maxReadCount} = this.state
    const {loading} = kafkaData
    const payload = kafkaData.result.payload || []
    const content = "".concat(...payload)
    return (
      <div className="form-search">
        <span>
          <h2>Kafka Reader</h2>
        </span>
        <Form autoComplete="off" layout="horizontal">
          <FormItem
            label="Topic"
            {...formItemLayout}
          >
            {getFieldDecorator('topic', {
              rules: [
                {
                  required: true,
                  message: '请选择topic'
                }
              ]
            })(
              <Select
                mode="combobox"
                placeholder="Select topic"
                optionFilterProp="children"
                onChange={this.handleTopicChange}
                filterOption={(input, option) => option.props.children.toLowerCase().indexOf(input.toLowerCase()) >= 0}
              >
                {topicList.map(item => (
                  <Option value={item} key={item}>
                    {item}
                  </Option>
                ))}
              </Select>
            )}
          </FormItem>
          <FormItem
            label={<FormattedMessage
              id="app.components.toolset.kafkaReader.from"
              defaultMessage="读取位置"
            />}
            {...formItemLayout}
          >
            {getFieldDecorator('from', {
              rules: [
                {
                  required: true,
                  message: '请输入读取位置'
                },
                {
                  pattern: /\d+/,
                  message: '请输入数字'
                }
              ]
            })(
              <Input
              />
            )}
            <font style={{marginLeft: 10}} color="gray">
              起始Offset：{beginOffset}，
              末尾Offset（不包含）：{endOffset}，
              最大读取条数：{maxReadCount}
            </font>
          </FormItem>
          <FormItem
            label={<FormattedMessage
              id="app.components.toolset.kafkaReader.length"
              defaultMessage="读取条数"
            />}
            {...formItemLayout}
          >
            {getFieldDecorator('length', {
              rules: [
                {
                  required: true,
                  message: '请输入读取条数'
                },
                {
                  pattern: /\d+/,
                  message: '请输入数字'
                }
              ]
            })(
              <Input
              />
            )}
          </FormItem>
          <FormItem
            label={<FormattedMessage
              id="app.components.toolset.kafkaReader.params"
              defaultMessage="过滤参数"
            />}
            {...formItemLayout}
          >
            {getFieldDecorator('params', {
              rules: [
              ]
            })(
              <Input
              />
            )}
          </FormItem>
          <FormItem
            wrapperCol={{
              xs: {span: 24, offset: 0},
              sm: {span: 16, offset: 4}
            }}
          >
            <Button loading={loading} onClick={this.handleRead} type="primary" htmlType="submit">
              <FormattedMessage
                id="app.components.toolset.kafkaReader.readKafka"
                defaultMessage="读取"
              />
            </Button>
          </FormItem>
        </Form>
        <TextArea value={content} wrap='off' autosize={{minRows:10, maxRows: 24}}/>
      </div>
    )
  }
}

KafkaReaderForm.propTypes = {
  form: PropTypes.object,
}
