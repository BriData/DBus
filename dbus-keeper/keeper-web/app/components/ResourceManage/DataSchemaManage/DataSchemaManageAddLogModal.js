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

  handleAddLogTableModalOk = (values) => {
    const {tableList} = this.state
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
          添加Table
          <Button style={{marginLeft: 10}} onClick={this.handleOpenAddLogTableModal}>添加表</Button>
        </span>}
      >
        <div>
          <Form>
            <Row>
              <Col span={8}>
                <FormItem  label={'dataSource'} {...formItemLayout}>
                  <Input
                    size="small"
                    type="text"
                    value={`${record.ds_type}/${record.ds_name}`}
                  />
                </FormItem>
              </Col>
              <Col span={8}>
                <FormItem label={'Schema'} {...formItemLayout}>
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
                <FormItem label={'target_topic'} {...formItemLayout}>
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
          <Table
            rowKey={record => `${record.tableName}`}
            columns={columns}
            dataSource={tableList}
            pagination={false}
            scroll={{x:800, y: 350}}
          />
          <DataSchemaAddLogSchemaTableModal
            visible={addLogTableModalVisible}
            key={addLogTableModalKey}
            defaultOutputTopic={addLogTableModalDefaultOutputTopic}
            onClose={this.handleCloseAddLogTableModal}
            onOk={this.handleAddLogTableModalOk}
          />
        </div>
      </Modal>
    )
  }
}

DataSchemaManageAddLogModal.propTypes = {}
