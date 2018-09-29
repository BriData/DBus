import React, {PropTypes, Component} from 'react'
import {Form, Select, Table, Row, Col, Button, Checkbox} from 'antd'
import {FormattedMessage} from 'react-intl'
// 导入样式
import styles from './res/styles/index.less'
import Request from "@/app/utils/request";

import OperatingButton from '@/app/components/common/OperatingButton'
import EditableCell from './EditableCell'

const FormItem = Form.Item
const Option = Select.Option

export default class NotifyConfigForm extends Component {
  constructor(props) {
    super(props)
    this.state = {
      notify: this._getNotify(props)
    }
  }

  handleRandom = key =>
    `${Math.random()
      .toString(32)
      .substr(3, 8)}${key || ''}`;

  _getNotify = props => {
    const {config} = props
    const {additionalNotify} = config
    const notify = []
    for (let key in additionalNotify) {
      notify.push({
        key: this.handleRandom(key+'.notify'),
        schema: key,
        SMSNo: additionalNotify[key].SMSNo,
        UseSMS: additionalNotify[key].UseSMS,
        Email: additionalNotify[key].Email,
        UseEmail: additionalNotify[key].UseEmail,
      })
    }
    return notify
  }

  componentWillReceiveProps = nextProps => {
    this.setState({notify: this._getNotify(nextProps)})
  }

  handleEditableCellSave = (index, param, value) => {
    const {notify} = this.state
    notify[index][param] = value
    this.setState({notify}, this.handleSave)
  }

  handleAdd = () => {
    const {notify} = this.state
    notify.push({
      key: this.handleRandom('ds/schema.notify'),
      schema: `ds/schema_${this.handleRandom()}`,
      SMSNo: '12344445555',
      UseSMS: 'N',
      Email: 'example@example.com',
      UseEmail: 'Y',
    })
    this.setState({notify: [...notify]}, this.handleSave)
  }

  handleDelete = index => {
    const {notify} = this.state
    notify.splice(index, 1)
    this.setState({notify: [...notify]}, this.handleSave)
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

  renderCheckbox = (param) => (text, record, index) => {
    return (
      <div
        title={text}
        className={styles.ellipsis}
      >
        <Checkbox
          checked={text === 'Y'}
          onChange={e => this.handleEditableCellSave(index, param, e.target.checked ? 'Y' : 'N')}
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
    const {notify} = this.state
    const additionalNotify = {}
    notify.forEach(item => additionalNotify[item.schema] = {
      SMSNo: item.SMSNo,
      UseSMS: item.UseSMS,
      Email: item.Email,
      UseEmail: item.UseEmail,
    })

    const {onValueChange} = this.props
    onValueChange(additionalNotify, 'additionalNotify')

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
          id="app.common.user.phone"
          defaultMessage="手机号"
        />,
        dataIndex: 'SMSNo',
        key: 'SMSNo',
        render: this.renderComponent(this.renderNomal('SMSNo'))
      },
      {
        title: <FormattedMessage
          id="app.components.configCenter.heartbeatConfig.enableSMS"
          defaultMessage="启用短信"
        />,
        width: 100,
        dataIndex: 'UseSMS',
        key: 'UseSMS',
        render: this.renderComponent(this.renderCheckbox('UseSMS'))
      },
      {
        title: <FormattedMessage
          id="app.common.user.email"
          defaultMessage="邮箱"
        />,
        dataIndex: 'Email',
        key: 'Email',
        render: this.renderComponent(this.renderNomal('Email'))
      },
      {
        title: <FormattedMessage
          id="app.components.configCenter.heartbeatConfig.enableEmail"
          defaultMessage="启用邮箱"
        />,
        width: 100,
        dataIndex: 'UseEmail',
        key: 'UseEmail',
        render: this.renderComponent(this.renderCheckbox('UseEmail'))
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

    const {notify} = this.state
    return (
      <div>
        <Table
          dataSource={notify}
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

NotifyConfigForm.propTypes = {}
