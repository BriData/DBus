/**
 * @author xiancangao
 * @description  基本信息设置
 */
import React, {Component, PropTypes} from 'react'
import {Button, Form, Input, Select, Tabs} from 'antd'
import JSONTree from 'react-json-tree'
import dateFormat from 'dateformat'
import {FormattedMessage} from 'react-intl'
import BatchControlMessageForm from './BatchControlMessageForm'

const TabPane = Tabs.TabPane;
const Option = Select.Option
const FormItem = Form.Item
@Form.create({warppedComponentRef: true})
export default class ControlMessageForm extends Component {
  constructor(props) {
    super(props)
    this.messageTypeGroup = [
      {value: 'LOG_PROCESSOR_RELOAD_CONFIG', text: 'Reload Log-Processor'},
      {value: 'EXTRACTOR_RELOAD_CONF', text: 'Reload Extractor'},
      {value: 'DISPATCHER_RELOAD_CONFIG', text: 'Reload Dispatcher'},
      {value: 'APPENDER_RELOAD_CONFIG', text: 'Reload Appender'},
      {value: 'FULL_DATA_PULL_RELOAD_CONF', text: 'Reload Splitter-Fullpuller'},
      {value: 'ROUTER_RELOAD', text: 'Reload Router'},
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
    let topology = {}
    const date = new Date()
    if (messageType === 'ROUTER_RELOAD') {
      const topologyList = this.props.topologyList
      topologyList && Object.values(topologyList).forEach(topo => {
        topo.id === dsId && (topology = topo)
      })
      this.setState({
        json: {
          from: 'dbus-web',
          id: date.getTime(),
          payload: {},
          timestamp: dateFormat(date, 'yyyy-mm-dd HH:MM:ss.l'),
          type: messageType
        }
      })
    } else if (messageType !== 'HEARTBEAT_RELOAD_CONFIG') {
      const dataSourceList = this.props.dataSourceList || []
      dataSourceList.forEach(ds => {
        ds.dsId === dsId && (dataSource = {
          dsName: ds.ds_name,
          dsType: ds.ds_type
        })
      })
      this.setState({
        json: {
          from: 'dbus-web',
          id: date.getTime(),
          payload: dataSource || {},
          timestamp: dateFormat(date, 'yyyy-mm-dd HH:MM:ss.l'),
          type: messageType
        }
      })
    } else {
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

  }

  handleMessageTypeChange = messageType => {
    this.setState({
        messageType,
        isHeartBeat: messageType === 'HEARTBEAT_RELOAD_CONFIG',
        isRouter: messageType === 'ROUTER_RELOAD'
      },
      this.buildTemplate)
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

  handleTopoChange = topoId => {
    const topologyList = this.props.topologyList
    topologyList && Object.values(topologyList).forEach(topo => {
      if (topo.topoId === topoId) {
        const ctrlTopic = `${topo.topoName}_ctrl`
        console.log(ctrlTopic)
        this.props.form.setFieldsValue({ctrlTopic: ctrlTopic})
        this.setState({ctrlTopic: ctrlTopic}, this.buildTemplate)
      }
    })
  }

  handleCtrlTopicChange = ctrlTopic => {
    this.setState({ctrlTopic})
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

  render() {
    const {getFieldDecorator} = this.props.form
    const {isHeartBeat, isRouter} = this.state
    const formItemLayout = {
      labelCol: {span: 4},
      wrapperCol: {span: 10}
    }

    const dataSourceList = this.props.dataSourceList || []
    const topologyList = this.props.topologyList
    const {json} = this.state

    const theme = {
      scheme: 'monokai',
      base00: '#272822'
    }
    return (
      <div className="form-search">
        <Tabs
          defaultActiveKey="global"
        >
          <TabPane
            tab={<FormattedMessage
              id="app.components.toolset.controlMessage.controlMessage"
              defaultMessage="控制消息"
            />}
            key="controlMessage"
          >
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
              {!isHeartBeat && !isRouter && (
                <FormItem
                  label={<FormattedMessage
                    id="app.components.toolset.controlMessage.dataSource"
                    defaultMessage="数据源"
                  />}
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
              {isRouter && (
                <FormItem
                  label={<FormattedMessage
                    id="app.common.topo"
                    defaultMessage="拓扑"
                  />}
                  {...formItemLayout}
                >
                  {getFieldDecorator('topoId', {
                    rules: [
                      {
                        required: !isHeartBeat,
                        message: '请选择拓扑'
                      }
                    ]
                  })(
                    <Select
                      showSearch
                      optionFilterProp="children"
                      onChange={this.handleTopoChange}
                      placeholder="select a topology"
                      filterOption={(input, option) => option.props.children.toLowerCase().indexOf(input.toLowerCase()) >= 0}
                    >
                      {topologyList && Object.values(topologyList).map(item => (
                        <Option value={item.topoId} key={item.topoId}>
                          {item.topoName}
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
                <JSONTree data={json} theme={theme}/>
              </FormItem>
              <FormItem
                wrapperCol={{
                  xs: {span: 24, offset: 0},
                  sm: {span: 16, offset: 4}
                }}
              >
                <Button onClick={this.handleSend} type="primary" htmlType="submit">
                  <FormattedMessage
                    id="app.components.toolset.controlMessage.sendControlMessage"
                    defaultMessage="发送控制消息"
                  />
                </Button>
              </FormItem>
              <FormItem
                wrapperCol={{
                  xs: {span: 24, offset: 0},
                  sm: {span: 16, offset: 4}
                }}
              >
                <Button onClick={this.handleReadZk} type="primary" htmlType="submit">
                  <FormattedMessage
                    id="app.components.toolset.controlMessage.readZK"
                    defaultMessage="读取发送结果"
                  />
                </Button>
              </FormItem>
            </Form>
          </TabPane>
          <TabPane
            tab={<FormattedMessage
              id="app.components.toolset.controlMessage.batchControlMessage"
              defaultMessage="批量控制消息"
            />}
            key="start"
          >
            <BatchControlMessageForm
              messageTypeGroup={this.messageTypeGroup}
            />
          </TabPane>
        </Tabs>
      </div>
    )
  }
}

ControlMessageForm.propTypes = {
  form: PropTypes.object,
  messageTypeChanged: PropTypes.func
}
