/**
 * @author xiancangao
 * @description  基本信息设置
 */
import React, {PropTypes, Component} from 'react'
import {Form, Input, Button, Select} from 'antd'
import JSONTree from 'react-json-tree'
import dateFormat from 'dateformat'

const Option = Select.Option
const FormItem = Form.Item
@Form.create({warppedComponentRef: true})
export default class ControlMessageForm extends Component {
  constructor (props) {
    super(props)
    this.messageTypeGroup = [
      {value: 'LOG_PROCESSOR_RELOAD_CONFIG', text: 'Reload Log-Processor'},
      {value: 'EXTRACTOR_RELOAD_CONF', text: 'Reload Extractor'},
      {value: 'DISPATCHER_RELOAD_CONFIG', text: 'Reload Dispatcher'},
      {value: 'APPENDER_RELOAD_CONFIG', text: 'Reload Appender'},
      {value: 'FULL_DATA_PULL_RELOAD_CONF', text: 'Reload Splitter-Fullpuller'},
      {value: 'HEARTBEAT_RELOAD_CONFIG', text: 'Reload Heartbeat'}
    ]
    this.state = {
      messageType: null,
      dsId: null,
      ctrlTopic: null,
      json: {}
    }
  }

  buildTemplate = () => {
    const {messageType, dsId} = this.state
    let dataSource = {}
    const date = new Date()
    if (messageType !== 'HEARTBEAT_RELOAD_CONFIG') {
      const dataSourceList = this.props.dataSourceList || []
      dataSourceList.forEach(ds => {
        ds.dsId === dsId && (dataSource = {
          dsName: ds.ds_name,
          dsType: ds.ds_type
        })
      })
    }
    this.setState({
      json: {
        from: 'dbus-web',
        id: date.getTime(),
        payload: dataSource || {},
        timestamp: dateFormat(date, 'yyyy-mm-dd HH:MM:ss.l'),
        type: messageType
      }
    })
  }

  handleMessageTypeChange = messageType => {
    this.setState({messageType}, this.buildTemplate)
  }

  handleDsIdChange = dsId => {
    const dataSourceList = this.props.dataSourceList || []
    dataSourceList.forEach(dataSource => {
      if (dataSource.dsId === dsId) {
        this.props.form.setFieldsValue({ctrlTopic: dataSource.ctrl_topic})
        this.setState({dsId: dsId, ctrlTopic: dataSource.ctrl_topic}, this.buildTemplate)
      }
    })
  }

  handleCtrlTopicChange = ctrlTopic => {
    this.setState({ctrlTopic}, this.buildTemplate)
  }

  handleSend = () => {
    const {ctrlTopic, json} = this.state
    const data = {
      topic: ctrlTopic,
      message: JSON.stringify(json)
    }
    if (!data.topic) delete data.topic
    const {onSend} = this.props
    onSend(data)
  }

  handleReadZk = () => {
    const {onReadZk} = this.props
    const {messageType} = this.state
    onReadZk(messageType)
  }

  render () {
    const {getFieldDecorator, getFieldsValue} = this.props.form
    const {messageType} = getFieldsValue()
    const isHeartBeat = messageType === 'HEARTBEAT_RELOAD_CONFIG'
    const formItemLayout = {
      labelCol: {span: 4},
      wrapperCol: {span: 10}
    }

    const dataSourceList = this.props.dataSourceList || []
    const {json} = this.state

    const theme = {
      scheme: 'monokai',
      base00: '#272822'
    }
    return (
      <div className="form-search">
        <span>
          <h2>Control message</h2>
        </span>
        <Form autoComplete="off" layout="horizontal">
          <FormItem
            label="Message Type"
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
                showSearch
                placeholder="Select message type"
                onChange={this.handleMessageTypeChange}
                optionFilterProp="children"
                filterOption={(input, option) => option.props.children.toLowerCase().indexOf(input.toLowerCase()) >= 0}
              >
                {this.messageTypeGroup.map(item => (
                  <Option value={item.value} key={item.value}>
                    {item.text}
                  </Option>
                ))}
              </Select>
            )}
          </FormItem>
          {!isHeartBeat && (
            <FormItem
              label="Data Source"
              {...formItemLayout}
            >
              {getFieldDecorator('dsId', {
                rules: [
                  {
                    required: !isHeartBeat,
                    message: '请选择数据源'
                  }
                ]
              })(
                <Select
                  showSearch
                  optionFilterProp="children"
                  onChange={this.handleDsIdChange}
                  placeholder="select a data source"
                  filterOption={(input, option) => option.props.children.toLowerCase().indexOf(input.toLowerCase()) >= 0}
                >
                  {dataSourceList.map(item => (
                    <Option value={item.dsId ? item.dsId : null} key={`${item.dsId ? item.dsId : 'dsId'}`}>
                      {item.dsTypeName}
                    </Option>
                  ))}
                </Select>
              )}
            </FormItem>
          )}
          {!isHeartBeat && (
            <FormItem
              label="Ctrl Topic"
              {...formItemLayout}
            >
              {getFieldDecorator('ctrlTopic', {
                rules: [
                  {
                    required: !isHeartBeat,
                    message: '请输入ctrlTopic'
                  }
                ]
              })(
                <Input
                  onChange={({target: {value}}) => this.handleCtrlTopicChange(value)}
                />
              )}
            </FormItem>
          )}
          <FormItem
            wrapperCol={{
              xs: {span: 4, offset: 0},
              sm: {span: 10, offset: 4}
            }}
          >
            <JSONTree data={json} theme={theme} />
          </FormItem>
          <FormItem
            wrapperCol={{
              xs: {span: 24, offset: 0},
              sm: {span: 16, offset: 4}
            }}
          >
            <Button onClick={this.handleSend} type="primary" htmlType="submit">Send Control Message</Button>
          </FormItem>
          <FormItem
            wrapperCol={{
              xs: {span: 24, offset: 0},
              sm: {span: 16, offset: 4}
            }}
          >
            <Button onClick={this.handleReadZk} type="primary" htmlType="submit">Read Zk Node</Button>
          </FormItem>
        </Form>
      </div>
    )
  }
}

ControlMessageForm.propTypes = {
  form: PropTypes.object,
  messageTypeChanged: PropTypes.func
}
