import React, {PropTypes, Component} from 'react'
import {Form, Select, Input, Row, Col, Button, Tabs, Spin, Table, message , Icon} from 'antd'
const TabPane = Tabs.TabPane;
const Textarea = Input.TextArea
import AddLogSchemaTableModal from './AddLogSchemaTableModal'
// 导入样式
import styles from './res/styles/index.less'
import Request from "@/app/utils/request";
import dateFormat from 'dateformat'

const FormItem = Form.Item
const Option = Select.Option

@Form.create()
export default class AddLogSchemaTable extends Component {
  constructor(props) {
    super(props)
    this.state = {
      tableList: [],
      addLogTableModalKey: 'addLogTableModalKey',
      addLogTableModalVisible: false,
      addLogTableModalDefaultOutputTopic: null,

      nextLoading: false
    }
    this.tableWidth = [
      '33%',
      '33%',
      '33%',
    ]
  }

  componentWillMount = () => {
  }
  /**
   * @param render 传入一个render
   * @returns render 返回一个新的render
   * @description 统一处理render函数
   */
  renderComponent = render => (text, record, index) =>
    render(text, record, index);

  /**
   * @description 默认的render
   */
  renderNomal = (text, record, index) => (
    <div title={text} className={styles.ellipsis}>
      {text}
    </div>
  )

  handleNext = () => {
    const {addSchemaTableApi, dataSource} = this.props
    const schema = this.props.form.getFieldsValue()
    const {tableList} = this.state
    const data = {
      dataSource: {
        id: dataSource.id,
        dsName:dataSource.dsName,
        dsType:dataSource.dsType
      }
    }
    if(tableList.length === 0) {
      message.warn("未添加任何表")
      return
    }
    data.schemaAndTables = [
      {
        schema: {
          dsId: dataSource.id,
          dsName: dataSource.dsName,
          dsType: dataSource.dsType,
          schemaName: schema.schemaName,
          srcTopic: schema.srcTopic,
          targetTopic: schema.targetTopic,
        },
        tables: tableList.map(table => ({
          schemaName: schema.schemaName,
          tableName: table.tableName,
          physicalTableRegex: table.tableName,
          outputTopic: table.outputTopic,
        }))
      }
    ]

    this.setState({nextLoading: true})
    Request(addSchemaTableApi, {
      data: data,
      method: 'post' })
      .then(res => {
        if (res && res.status === 0) {
          this.handleAutoClone()
        } else {
          this.setState({nextLoading: false})
          message.warn(res.message)
        }
      })
      .catch(error => {
        this.setState({nextLoading: false})
        error.response && error.response.data && error.response.data.message
          ? message.error(error.response.data.message)
          : message.error(error.message)
      })
  }

  handleAutoClone = () => {
    const {cloneConfFromTemplateApi, dataSource} = this.props
    Request(cloneConfFromTemplateApi, {
      params: {
        dsName: dataSource.dsName,
        dsType: dataSource.dsType
      },
      method: 'get' })
      .then(res => {
        this.setState({nextLoading: false})
        if (res && res.status === 0) {
          message.success('自动克隆ZK完成')
          const {onAutoCloneZkSuccess} = this.props
          onAutoCloneZkSuccess()
        } else {
          this.setState({nextLoading: false})
          message.warn(res.message)
          const {onAutoCloneZkFail} = this.props
          onAutoCloneZkFail()
        }
      })
      .catch(error => {
        const {onAutoCloneZkFail} = this.props
        onAutoCloneZkFail()
        error.response.data && error.response.data.message
          ? message.error(error.response.data.message)
          : message.error(error.message)
      })
  }

  handleOpenAddLogTableModal = () => {
    const targetTopic = this.props.form.getFieldValue('targetTopic')
    this.setState({
      addLogTableModalVisible: true,
      addLogTableModalDefaultOutputTopic: targetTopic,
    })
  }

  handleAddLogTableModalOk = (values) => {
    const {tableList} = this.state
    const exist = tableList.some(table => table.tableName === values.tableName)
    if (exist) {
      message.warn('列表中已存在相同表名')
      return
    }
    tableList.push({
      ...values,
      createTime: dateFormat(new Date(), 'yyyy-mm-dd HH:MM:ss.l'),
    })
    this.setState({tableList})
    this.handleCloseAddLogTableModal()
  }

  handleCloseAddLogTableModal = () => {
    this.setState({
      addLogTableModalKey: this.handleRandom('addLogTableModalKey'),
      addLogTableModalVisible: false,
    })
  }

  handleRandom = key =>
    `${Math.random()
      .toString(32)
      .substr(3, 8)}${key || ''}`

  render() {
    const { getFieldDecorator } = this.props.form
    const {dataSource} = this.props
    const columns = [
      {
        title: (
          'TableName'
        ),
        width: this.tableWidth[0],
        dataIndex: 'tableName',
        key: 'tableName',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          'OutputTopic'
        ),
        width: this.tableWidth[1],
        dataIndex: 'outputTopic',
        key: 'outputTopic',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          'CreateTime'
        ),
        width: this.tableWidth[2],
        dataIndex: 'createTime',
        key: 'createTime',
        render: this.renderComponent(this.renderNomal)
      }
    ]

    const formItemLayout = {
      labelCol: {
        span: 7
      },
      wrapperCol: {
        span: 17
      },
      style: {
        marginBottom: 0
      }
    }
    const tailFormItemLayout = {
      wrapperCol: {
        span: 4,
        offset: 20,
      }
    }

    const {addLogTableModalKey, addLogTableModalVisible, addLogTableModalDefaultOutputTopic} = this.state
    const {tableList} = this.state
    const {nextLoading} = this.state
    return (

      <div>
        <Form>
          <Row>
            <Col span={8}>
              <FormItem  label={'dataSource'} {...formItemLayout}>
                <Input
                  size="small"
                  type="text"
                  value={`${dataSource.dsType}/${dataSource.dsName}`}
                />
              </FormItem>
            </Col>
            <Col span={8}>
              <FormItem label={'Schema'} {...formItemLayout}>
                {getFieldDecorator('schemaName', {
                  initialValue: `${dataSource.dsName}_schema`,
                  rules: [
                    {
                      required: true,
                      message: 'Schema不能为空'
                    }
                  ]
                })(<Input
                  size="small"
                  type="text"
                />)}
              </FormItem>
            </Col>
            <Col span={8}>
              <FormItem label={'Description'} {...formItemLayout}>
                {getFieldDecorator('description', {
                })(<Input
                  size="small"
                  type="text"
                />)}
              </FormItem>
            </Col>
          </Row>
          <Row style={{marginTop: -5}}>
            <Col span={8}>
              <FormItem label={'status'} {...formItemLayout}>
                {getFieldDecorator('status', {
                  initialValue: `active`,
                  rules: [
                    {
                      required: true,
                      message: 'status不能为空'
                    }
                  ]
                })(<Input
                  size="small"
                  type="text"
                />)}
              </FormItem>
            </Col>
            <Col span={8}>
              <FormItem label={'src_topic'} {...formItemLayout}>
                {getFieldDecorator('srcTopic', {
                  initialValue: `${dataSource.dsName}.${dataSource.dsName}_schema`,
                  rules: [
                    {
                      required: true,
                      message: 'src_topic不能为空'
                    }
                  ]
                })(<Input
                  size="small"
                  type="text"
                />)}
              </FormItem>
            </Col>
            <Col span={8}>
              <FormItem label={'target_topic'} {...formItemLayout}>
                {getFieldDecorator('targetTopic', {
                  initialValue: `${dataSource.dsName}.${dataSource.dsName}_schema.result`,
                  rules: [
                    {
                      required: true,
                      message: 'target_topic不能为空'
                    }
                  ]
                })(<Input
                  size="small"
                  type="text"
                />)}
              </FormItem>
            </Col>
          </Row>
        </Form>
        <Table
          rowKey={record => `${record.tableName}`}
          columns={columns}
          dataSource={tableList}
          pagination={false}
        />
        <Form autoComplete="off" style={{marginTop: 10}}>
          <FormItem {...tailFormItemLayout}>
            <Button style={{marginRight: 10}} onClick={this.handleOpenAddLogTableModal}>添加表</Button>
            <Button loading={nextLoading} type="primary" onClick={this.handleNext}>下一步</Button>
          </FormItem>
        </Form>
        <AddLogSchemaTableModal
          visible={addLogTableModalVisible}
          key={addLogTableModalKey}
          defaultOutputTopic={addLogTableModalDefaultOutputTopic}
          onClose={this.handleCloseAddLogTableModal}
          onOk={this.handleAddLogTableModalOk}
        />
      </div>
    )
  }
}

AddLogSchemaTable.propTypes = {}
