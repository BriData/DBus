import React, {PropTypes, Component} from 'react'
import {Button, Row, Col, Modal, Form, Select, Input, Spin, Table, message} from 'antd'
import {FormattedMessage} from 'react-intl'
import OperatingButton from '@/app/components/common/OperatingButton'
import DataSchemaAddLogSchemaTableModal from './DataSchemaAddLogSchemaTableModal'
// 导入样式
import styles from './res/styles/index.less'
import Request from "@/app/utils/request";
import dateFormat from 'dateformat'
const FormItem = Form.Item
const Option = Select.Option
const TextArea = Input.TextArea

@Form.create()
export default class DataSchemaManageAddLogModal extends Component {
  constructor(props) {
    super(props)
    this.tableWidth = [
      '33%',
      '33%',
      '33%',
    ]
    this.state = {
      tableList: [],
      addLogTableModalKey: 'addLogTableModalKey',
      addLogTableModalVisible: false,
      addLogTableModalDefaultOutputTopic: null,
    }
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

  handleOk = () => {
    const {onClose, addApi, record} = this.props
    const {tableList} = this.state
    const data = {
      dataSource: {
        id: record.ds_id,
        dsName:record.ds_name,
        dsType:record.ds_type
      }
    }
    if(tableList.length === 0) {
      message.warn("未添加任何表")
      return
    }
    data.schemaAndTables = [
      {
        schema: {
          dsId: record.ds_id,
          dsName: record.ds_name,
          dsType: record.ds_type,
          schemaName: record.schema_name,
          srcTopic: record.src_topic,
          targetTopic: record.target_topic,
        },
        tables: tableList.map(table => ({
          schemaName: record.schema_name,
          tableName: table.tableName,
          physicalTableRegex: table.tableName,
          outputTopic: table.outputTopic,
        }))
      }
    ]

    Request(addApi, {
      data: data,
      method: 'post' })
      .then(res => {
        if (res && res.status === 0) {
          message.success(res.message)
          onClose()
        } else {
          message.warn(res.message)
        }
      })
      .catch(error => {
        error.response && error.response.data && error.response.data.message
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

  handleAddLogTable = () => {
    this.props.form.validateFieldsAndScroll((err, values) => {
      if (!err) {
        this.handleAddLogTableModalOk({
          tableName: values.tableName,
          outputTopic: values.outputTopic
        })
        this.props.form.setFieldsValue({tableName:null})
      }
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
    // this.handleCloseAddLogTableModal()
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
    const columns = [
      {
        title: (
          <FormattedMessage
            id="app.components.resourceManage.dataTableName"
            defaultMessage="表名"
          />
        ),
        width: this.tableWidth[0],
        dataIndex: 'tableName',
        key: 'tableName',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectTable.outputTopic"
            defaultMessage="输出Topic"
          />
        ),
        width: this.tableWidth[1],
        dataIndex: 'outputTopic',
        key: 'outputTopic',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.common.createTime"
            defaultMessage="创建时间"
          />
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

    const addTableFormItemLayout = {
      labelCol: {
        span: 8
      },
      wrapperCol: {
        span: 16
      },
      style: {
        marginBottom: 3
      }
    }

    const addTableFormButtonItemLayout = {
      wrapperCol: {
        offset: 2,
        span: 12
      },
      style: {
        marginBottom: 3
      }
    }

    const {addLogTableModalKey, addLogTableModalVisible, addLogTableModalDefaultOutputTopic} = this.state
    const {tableList} = this.state

    const {visible, key, record, onClose} = this.props
    return (
      <Modal
        visible={visible}
        maskClosable={false}
        key={key}
        width={1000}
        onCancel={onClose}
        onOk={this.handleOk}
        title={<span>
          <FormattedMessage
            id="app.components.resourceManage.dataSchema.addTable"
            defaultMessage="添加表"
          />
        </span>}
      >
        <div>
          <Form>
            <Row>
              <Col span={8}>
                <FormItem  label={<FormattedMessage
                  id="app.components.resourceManage.dataSourceName"
                  defaultMessage="数据源名称"
                />} {...formItemLayout}>
                  <Input
                    size="small"
                    type="text"
                    value={`${record.ds_type}/${record.ds_name}`}
                  />
                </FormItem>
              </Col>
              <Col span={8}>
                <FormItem label={<FormattedMessage
                  id="app.components.resourceManage.dataSchemaName"
                  defaultMessage="Schema名称"
                />} {...formItemLayout}>
                  {getFieldDecorator('schemaName', {
                    initialValue: `${record.ds_name}_schema`,
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
                <FormItem label={<FormattedMessage
                  id="app.common.description"
                  defaultMessage="描述"
                />} {...formItemLayout}>
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
                <FormItem label={<FormattedMessage
                  id="app.common.status"
                  defaultMessage="状态"
                />} {...formItemLayout}>
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
                <FormItem label={<FormattedMessage
                  id="app.components.resourceManage.sourceTopic"
                  defaultMessage="源Topic"
                />} {...formItemLayout}>
                  {getFieldDecorator('srcTopic', {
                    initialValue: `${record.ds_name}.${record.ds_name}_schema`,
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
                <FormItem label={<FormattedMessage
                  id="app.components.resourceManage.targetTopic"
                  defaultMessage="目标Topic"
                />} {...formItemLayout}>
                  {getFieldDecorator('targetTopic', {
                    initialValue: `${record.ds_name}.${record.ds_name}_schema.result`,
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
          <Form autoComplete="off">
            <Row style={{border: "1px solid #e9e9e9"}}>
              <Col span={10}>
                <FormItem label={<FormattedMessage
                  id="app.components.resourceManage.dataTableName"
                  defaultMessage="表名"
                />} {...addTableFormItemLayout}>
                  {getFieldDecorator('tableName', {
                    initialValue: null,
                    rules: [
                      {
                        required: true,
                        message: 'TableName不能为空',
                        whitespace: true
                      }
                    ]
                  })(<Input
                    type="text"
                    size="small"
                  />)}
                </FormItem>
              </Col>
              <Col span={10}>
                <FormItem label={<FormattedMessage
                  id="app.components.projectManage.projectTable.outputTopic"
                  defaultMessage="输出Topic"
                />} {...addTableFormItemLayout}>
                  {getFieldDecorator('outputTopic', {
                    initialValue: `${record.ds_name}.${record.ds_name}_schema.result`,
                    rules: [
                      {
                        required: true,
                        message: 'OutputTopic不能为空',
                        whitespace: true
                      }
                    ]
                  })(<Input
                    type="text"
                    size="small"
                  />)}
                </FormItem>
              </Col>
              <Col span={2}>
                <FormItem {...addTableFormButtonItemLayout}>
                  <Button onClick={this.handleAddLogTable} type="primary" size="small">
                    <FormattedMessage
                      id="app.common.add"
                      defaultMessage="添加"
                    />
                  </Button>
                </FormItem>
              </Col>
            </Row>
          </Form>

          <Table
            rowKey={record => `${record.tableName}`}
            columns={columns}
            dataSource={tableList}
            pagination={false}
            scroll={{x:800, y: 350}}
          />
          {/*<DataSchemaAddLogSchemaTableModal
            visible={addLogTableModalVisible}
            key={addLogTableModalKey}
            defaultOutputTopic={addLogTableModalDefaultOutputTopic}
            onClose={this.handleCloseAddLogTableModal}
            onOk={this.handleAddLogTableModalOk}
          />*/}
        </div>
      </Modal>
    )
  }
}

DataSchemaManageAddLogModal.propTypes = {}
