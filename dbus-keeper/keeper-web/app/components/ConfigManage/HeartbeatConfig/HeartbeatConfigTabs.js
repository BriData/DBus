import React, {Component} from 'react'
import {Form, Select, Tabs} from 'antd'
import {FormattedMessage} from 'react-intl'
// 导入组件
import BasicConfigForm from './BasicConfigForm'
import AdvancedConfigForm from './AdvancedConfigForm'
import TimeoutConfigForm from './TimeoutConfigForm'
import NotifyConfigForm from './NotifyConfigForm'

const TabPane = Tabs.TabPane;


const FormItem = Form.Item
const Option = Select.Option

export default class HeartbeatConfigTabs extends Component {
  constructor(props) {
    super(props)
    this.state = {
      config: {}
    }
  }

  componentWillReceiveProps = nextProps => {
    const {config} = nextProps
    this.setState({config})
  }

  handleValueChange = (value, key) => {
    const {config} = this.state
    config[key] = value
    this.setState({config})
  }

  handleCheckboxChange = (value, key) => {
    const {config} = this.state
    config[key] = (value ? 'Y' : 'N')
    this.setState({config})
  }

  handleSave = () => {
    const {onSave} = this.props
    const {config} = this.state
    onSave(JSON.stringify(config, null, '\t'))
  }

  handleSendMailTest = () => {
    const {onSendMailTest} = this.props
    const {config} = this.state
    onSendMailTest(JSON.stringify(config, null, '\t'))
  }

  render() {
    const {config} = this.state
    const {allDataSchemaList, onSendMailTest} = this.props
    return (
      <div>
        <Tabs defaultActiveKey="basicConfig">
          <TabPane tab={<FormattedMessage
            id="app.components.configCenter.heartbeatConfig.basicConfig"
            defaultMessage="基础配置"
          />} key="basicConfig">
            <BasicConfigForm
              config={config}
              onValueChange={this.handleValueChange}
              onCheckboxChange={this.handleCheckboxChange}
              onSave={this.handleSave}
              onSendMailTest={this.handleSendMailTest}
            />
          </TabPane>
          <TabPane tab={<FormattedMessage
            id="app.components.configCenter.heartbeatConfig.advancedConfig"
            defaultMessage="高级配置"
          />} key="advancedConfig">
            <AdvancedConfigForm
              config={config}
              onValueChange={this.handleValueChange}
              onSave={this.handleSave}
            />
          </TabPane>
          <TabPane tab={<FormattedMessage
            id="app.components.configCenter.heartbeatConfig.timeoutConfig"
            defaultMessage="超时补充配置"
          />} key="timeoutConfig">
            <TimeoutConfigForm
              config={config}
              allDataSchemaList={allDataSchemaList}
              onValueChange={this.handleValueChange}
              onSave={this.handleSave}
            />
          </TabPane>
          <TabPane tab={<FormattedMessage
            id="app.components.configCenter.heartbeatConfig.notifyConfig"
            defaultMessage="通知补充配置"
          />} key="notifyConfig">
            <NotifyConfigForm
              config={config}
              allDataSchemaList={allDataSchemaList}
              onValueChange={this.handleValueChange}
              onSave={this.handleSave}
            />
          </TabPane>
        </Tabs>
      </div>
    )
  }
}

HeartbeatConfigTabs.propTypes = {}
