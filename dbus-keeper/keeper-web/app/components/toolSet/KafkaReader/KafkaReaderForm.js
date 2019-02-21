/**
 * @author xiancangao
 * @description  基本信息设置
 */
import React, {PropTypes, Component} from 'react'
import {Form, Row, Col, message, Input, Button, Select} from 'antd'
import JSONTree from 'react-json-tree'
import {FormattedMessage} from 'react-intl'
import dateFormat from 'dateformat'
import {KAFKA_READER_GET_OFFSET_RANGE_API} from '@/app/containers/toolSet/api'
import Request from "@/app/utils/request";
import styles from "./res/styles/index.less";

const TextArea = Input.TextArea
const Option = Select.Option
const FormItem = Form.Item
@Form.create({warppedComponentRef: true})
export default class KafkaReaderForm extends Component {
  constructor(props) {
    super(props)
    this.state = {
      beginOffset: 0,
      endOffset: 0,
      maxReadCount: 0
    }
  }

  componentWillMount = () => {
    const {record} = this.props
    if (record && record.outputTopic) {
      this.handleTopicChange(record.outputTopic)
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

  handleRefresh = () => {
    this.handleTopicChange(this.props.form.getFieldValue('topic'))
  }

  handleTopicChange = topic => {
    const {topicList, record} = this.props
    if (record && record.outputTopic && record.outputTopic === topic) {
      this.handleReadOffset(topic)
    } else if (topicList.some(t => t === topic)) {
      this.handleReadOffset(topic)
    }
  }

  handleReadOffset = topic => {
    Request(KAFKA_READER_GET_OFFSET_RANGE_API, {
      params: {topic},
      method: 'get'
    })
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

  render() {
    const {getFieldDecorator} = this.props.form
    const formItemLayout = {
      labelCol: {span: 8},
      wrapperCol: {span: 16}
    }
    const {record, topicList, kafkaData} = this.props
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
          <Row>
            <Col span={14}>
              <FormItem
                label="Topic"
                {...formItemLayout}
              >
                {getFieldDecorator('topic', {
                  initialValue: record && record.outputTopic,
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
            </Col>
            <Col span={4}>
              <Button style={{marginLeft: 10}} onClick={this.handleRefresh} type="primary">
                <FormattedMessage
                  id="app.common.refresh"
                  defaultMessage="刷新"
                />
              </Button>
            </Col>
          </Row>
          <Row>
            <Col span={14}>
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
                  <FormattedMessage
                    id="app.components.toolset.kafkaReader.beginOffset"
                    defaultMessage="起始Offset"
                  />：{beginOffset}，
                  <FormattedMessage
                    id="app.components.toolset.kafkaReader.endOffset"
                    defaultMessage="末尾Offset（不包含）"
                  />：{endOffset}，
                  <FormattedMessage
                    id="app.components.toolset.kafkaReader.maxReadCount"
                    defaultMessage="最大读取条数"
                  />：{maxReadCount}
                </font>
              </FormItem>
            </Col>
          </Row>
          <Row>
            <Col span={14}>
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
            </Col>
          </Row>
          <Row>
            <Col span={14}>
              <FormItem
                label={<FormattedMessage
                  id="app.components.toolset.kafkaReader.params"
                  defaultMessage="包含字符串"
                />}
                {...formItemLayout}
              >
                {getFieldDecorator('params', {
                  initialValue: record && record.outputTopic && `${record.dsName}!${record.topoName}.${record.schemaName}.${record.tableName}`,
                  rules: []
                })(
                  <Input
                  />
                )}
              </FormItem>
            </Col>
          </Row>
          <Row>
            <Col span={14}>
              <FormItem
                label={<FormattedMessage
                  id="app.components.toolset.kafkaReader.negParams"
                  defaultMessage="不包含字符串"
                />}
                {...formItemLayout}
              >
                {getFieldDecorator('negParams', {
                  initialValue: 'data_increment_heartbeat',
                  rules: []
                })(
                  <Input
                  />
                )}
              </FormItem>
            </Col>
          </Row>
          <Row>
            <Col span={14}>
              <FormItem
                wrapperCol={{
                  xs: {span: 24, offset: 0},
                  sm: {span: 16, offset: 8}
                }}
              >
                <Button loading={loading} onClick={this.handleRead} type="primary" htmlType="submit">
                  <FormattedMessage
                    id="app.components.toolset.kafkaReader.readKafka"
                    defaultMessage="读取"
                  />
                </Button>
              </FormItem>
            </Col>
          </Row>
        </Form>
        <div style={{background: "#fff",padding: "10px", border: "1px solid #ddd"}}>
          {
            content ?
              (<div
                style={{overflow: "auto", maxHeight: "350px"}}
                dangerouslySetInnerHTML={{__html: content.replace(/\n/g, '<br />')}}
              />) :
              (<div className={styles['noData']}><FormattedMessage
                id="app.components.toolset.kafkaReader.noData"
                defaultMessage="暂无数据"
              /></div>)
          }
        </div>

      </div>
    )
  }
}

KafkaReaderForm.propTypes = {
  form: PropTypes.object,
}
