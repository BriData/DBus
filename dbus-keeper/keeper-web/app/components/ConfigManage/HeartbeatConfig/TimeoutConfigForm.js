import React, {PropTypes, Component} from 'react'
import {Form, Select, Table, Row, Col, Button} from 'antd'
import {FormattedMessage} from 'react-intl'

// 导入样式
import styles from './res/styles/index.less'
import Request from "@/app/utils/request";

import OperatingButton from '@/app/components/common/OperatingButton'
import EditableCell from './EditableCell'

const FormItem = Form.Item
const Option = Select.Option

export default class TimeoutConfigForm extends Component {
  constructor(props) {
    super(props)
    this.state = {
      timeout: this._getTimeout(props)
    }
  }

  _getTimeout = props => {
    const {config} = props
    const {heartBeatTimeoutAdditional} = config
    const timeout = []
    for (let key in heartBeatTimeoutAdditional) {
      timeout.push({
        key: this.handleRandom(key+'.timeout'),
        schema: key,
        startTime: heartBeatTimeoutAdditional[key].startTime,
        endTime: heartBeatTimeoutAdditional[key].endTime,
        heartBeatTimeout: heartBeatTimeoutAdditional[key].heartBeatTimeout,
      })
    }
    return timeout
  }

  handleRandom = key =>
    `${Math.random()
      .toString(32)
      .substr(3, 8)}${key || ''}`;

  componentWillReceiveProps = nextProps => {
    this.setState({timeout: this._getTimeout(nextProps)})
  }

  handleEditableCellSave = (index, param, value) => {
    const {timeout} = this.state
    timeout[index][param] = value
    this.setState({timeout: timeout}, this.handleSave)
  }

  handleAdd = () => {
    const {timeout} = this.state
    timeout.push({
      key: this.handleRandom('ds/schema.timeout'),
      schema: `ds/schema_${this.handleRandom()}`,
      startTime: '0:00',
      endTime: '0:00',
      heartBeatTimeout: 600000,
    })
    this.setState({timeout: [...timeout]}, this.handleSave)
  }

  handleDelete = index => {
    const {timeout} = this.state
    timeout.splice(index, 1)
    this.setState({timeout: [...timeout]}, this.handleSave)
  }

  renderComponent = render => (text, record, index) =>
    render(text, record, index)

  renderNomal = (param) => (text, record, index) => {
    const {allDataSchemaList} = this.props
    return (
      <div
        title={text}
        className={styles.ellipsis}
      >
        <EditableCell
          allDataSchemaList={allDataSchemaList}
          value={text}
          index={index}
          param={param}
          onSave={this.handleEditableCellSave}
        />
      </div>
    )
  }

  renderOperating = (text, record, index) => (
    <div>
      <OperatingButton onClick={() => this.handleDelete(index)} icon='delete'>
        <FormattedMessage id="app.common.delete" defaultMessage="删除"/>
      </OperatingButton>
    </div>
  )

  handleSave = () => {
    const {timeout} = this.state
    const heartBeatTimeoutAdditional = {}
    timeout.forEach(item => heartBeatTimeoutAdditional[item.schema] = {
      startTime: item.startTime,
      endTime: item.endTime,
      heartBeatTimeout: item.heartBeatTimeout,
    })

    const {onValueChange} = this.props
    onValueChange(heartBeatTimeoutAdditional, 'heartBeatTimeoutAdditional')

    const {onSave} = this.props
    onSave()
  }

  render() {
    const columns = [
      {
        title: <FormattedMessage
          id="app.components.resourceManage.dataSchemaName"
          defaultMessage="Schema名称"
        />,
        dataIndex: 'schema',
        key: 'schema',
        render: this.renderComponent(this.renderNomal('schema'))
      },
      {
        title: <FormattedMessage
          id="app.components.configCenter.heartbeatConfig.startTime"
          defaultMessage="起始时间"
        />,
        dataIndex: 'startTime',
        key: 'startTime',
        render: this.renderComponent(this.renderNomal('startTime'))
      },
      {
        title: <FormattedMessage
          id="app.components.configCenter.heartbeatConfig.endTime"
          defaultMessage="截止时间"
        />,
        dataIndex: 'endTime',
        key: 'endTime',
        render: this.renderComponent(this.renderNomal('endTime'))
      },
      {
        title: <FormattedMessage
          id="app.components.configCenter.heartbeatConfig.heartBeatTimeout"
          defaultMessage="超时时间"
        />,
        dataIndex: 'heartBeatTimeout',
        key: 'heartBeatTimeout',
        render: this.renderComponent(this.renderNomal('heartBeatTimeout'))
      },
      {
        title: <FormattedMessage
          id="app.common.operate"
          defaultMessage="操作"
        />,
        width: 100,
        dataIndex: 'operation',
        key: 'operation',
        render: this.renderComponent(this.renderOperating)
      },
    ]

    const {timeout} = this.state
    return (
      <div>
        <Table
          dataSource={timeout}
          columns={columns}
          bordered
          pagination={false}
        />
        <Row style={{marginTop: 10}}>
          <Col span={24} style={{textAlign: 'right'}}>
            <Button onClick={this.handleAdd}>
              <FormattedMessage
                id="app.common.add"
                defaultMessage="添加"
              />
            </Button>
          </Col>
        </Row>
      </div>
    )
  }
}

TimeoutConfigForm.propTypes = {}
