import React, {PropTypes, Component} from 'react'
import {Button, Row, Col, Modal, Form, Select, Input, Spin, Table, Icon, message} from 'antd'
import {FormattedMessage} from 'react-intl'
import OperatingButton from '@/app/components/common/OperatingButton'
// 导入样式
import styles from './res/styles/index.less'
import Request from "@/app/utils/request";

const FormItem = Form.Item
const Option = Select.Option
const TextArea = Input.TextArea

@Form.create()
export default class DataSchemaManageAddModal extends Component {
  constructor(props) {
    super(props)
    this.state = {
      selectedRows: [],

    }
    this.tableWidth = [
      '20%',
      '20%',
      '20%',
      '20%',
      '20%',
    ]
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

  handleSubmit = () => {
    const {onClose, addApi, record} = this.props
    const {selectedRows} = this.state
    const data = {
      dataSource: {
        id: record.ds_id,
        dsName:record.ds_name,
        dsType:record.ds_type
      }
    }
    if(selectedRows.length === 0) {
      message.warn("未选中任何表")
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
        tables: selectedRows.map(table => ({
          schemaName: record.schema_name,
          tableName: table.tableName,
          physicalTableRegex: table.physicalTableRegex,
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
      .catch(error => message.error(error))
  }

  handleRandom = key =>
    `${Math.random()
      .toString(32)
      .substr(3, 8)}${key || ''}`


  render() {
    const {visible, key, record, onClose, schemaTableResult} = this.props
    const loading = schemaTableResult.loading
    const schemaTable = schemaTableResult.result.payload
    let tableList = schemaTable && schemaTable.tables || []
    tableList = tableList.map(table => ({
      ...table,
      outputTopic: table.outputTopic || record.target_topic,
      physicalTableRegex: table.physicalTableRegex || table.tableName
    }))
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
            id="app.components.resourceManage.dataTableNameRegex"
            defaultMessage="表名正则"
          />
        ),
        width: this.tableWidth[1],
        dataIndex: 'physicalTableRegex',
        key: 'physicalTableRegex',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectTable.outputTopic"
            defaultMessage="输出Topic"
          />
        ),
        width: this.tableWidth[2],
        dataIndex: 'outputTopic',
        key: 'outputTopic',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.resourceManage.dataSource.incompatibleColumn"
            defaultMessage="不兼容的列"
          />
        ),
        width: this.tableWidth[2],
        dataIndex: 'incompatibleColumn',
        key: 'incompatibleColumn',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.resourceManage.dataSource.ignoreColumn"
            defaultMessage="忽略的列"
          />
        ),
        width: this.tableWidth[2],
        dataIndex: 'columnName',
        key: 'columnName',
        render: this.renderComponent(this.renderNomal)
      }
    ]
    const rowSelection = {
      onChange: (selectedRowKeys, selectedRows) => {
        this.setState({
          selectedRows: selectedRows.filter(record => !record.disable)
        })
      },
      getCheckboxProps: record => ({
        disabled: record.disable,
        defaultChecked: record.disable
      }),
    }

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
    return (
      <Modal
        className="top-modal"
        visible={visible}
        maskClosable={false}
        key={key}
        onCancel={onClose}
        onOk={this.handleSubmit}
        width={1000}
        title={<div>
          <FormattedMessage
            id="app.components.resourceManage.dataSchema.addTable"
            defaultMessage="添加表"
          />
        </div>
        }
      >
        <Form>
          <Row>
            <Col span={8}>
              <FormItem  label={<FormattedMessage
                id="app.components.resourceManage.dataSourceName"
                defaultMessage="数据源名称"
              />} {...formItemLayout}>
                <Input
                  readOnly
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
                <Input
                  readOnly
                  size="small"
                  type="text"
                  value={record.schema_name}
                />
              </FormItem>
            </Col>
            <Col span={8}>
              <FormItem label={<FormattedMessage
                id="app.common.description"
                defaultMessage="描述"
              />} {...formItemLayout}>
                <Input
                  readOnly
                  size="small"
                  type="text"
                  value={record.description}
                />
              </FormItem>
            </Col>
          </Row>
          <Row style={{marginTop: -5}}>
            <Col span={8}>
              <FormItem label={<FormattedMessage
                id="app.common.status"
                defaultMessage="状态"
              />} {...formItemLayout}>
                <Input
                  readOnly
                  size="small"
                  type="text"
                  value={record.status}
                />
              </FormItem>
            </Col>
            <Col span={8}>
              <FormItem label={<FormattedMessage
                id="app.components.resourceManage.sourceTopic"
                defaultMessage="源Topic"
              />} {...formItemLayout}>
                <Input
                  readOnly
                  size="small"
                  type="text"
                  value={record.src_topic}
                />
              </FormItem>
            </Col>
            <Col span={8}>
              <FormItem label={<FormattedMessage
                id="app.components.resourceManage.targetTopic"
                defaultMessage="目标Topic"
              />} {...formItemLayout}>
                <Input
                  readOnly
                  size="small"
                  type="text"
                  value={record.target_topic}
                />
              </FormItem>
            </Col>
          </Row>
        </Form>
        <Spin spinning={loading} tip="正在加载数据中...">
          {!loading ? (
            <Table
              rowKey={record => `${record.tableName}`}
              rowSelection={rowSelection}
              columns={columns}
              dataSource={tableList}
              pagination={false}
              scroll={{x:800, y: 350}}
            />
          ) : (
            <div style={{width: '100%', height: 100}}/>
          )}
        </Spin>
      </Modal>
    )
  }
}

DataSchemaManageAddModal.propTypes = {}
