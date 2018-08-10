import React, {PropTypes, Component} from 'react'
import {Form, Select, Tabs} from 'antd'
const TabPane = Tabs.TabPane;
// 导入组件
import BasicConfigForm from './BasicConfigForm'
import AdvancedConfigForm from './AdvancedConfigForm'
import TimeoutConfigForm from './TimeoutConfigForm'
import NotifyConfigForm from './NotifyConfigForm'


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
    onSave(JSON.stringify(config, null ,'\t'))
  }

  render() {
    const {config} = this.state
    const {allDataSchemaList} = this.props
    return (
      <div>
        <Tabs defaultActiveKey="basicConfig">
          <TabPane tab="基础配置" key="basicConfig">
            <BasicConfigForm
              config={config}
              onValueChange={this.handleValueChange}
              onCheckboxChange={this.handleCheckboxChange}
              onSave={this.handleSave}
            />
          </TabPane>
          <TabPane tab="高级配置" key="advancedConfig">
            <AdvancedConfigForm
              config={config}
              onValueChange={this.handleValueChange}
              onSave={this.handleSave}
            />
          </TabPane>
          <TabPane tab="超时补充配置" key="timeoutConfig">
            <TimeoutConfigForm
              config={config}
              allDataSchemaList={allDataSchemaList}
              onValueChange={this.handleValueChange}
              onSave={this.handleSave}
            />
          </TabPane>
          <TabPane tab="通知补充配置" key="notifyConfig">
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
