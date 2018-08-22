import React, {PropTypes, Component} from 'react'
import {Row, Col, Modal, Form, Select, Input, Spin, Table, Icon, message} from 'antd'
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
          'tableName'
        ),
        width: this.tableWidth[0],
        dataIndex: 'tableName',
        key: 'tableName',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          'physicalTableRegex'
        ),
        width: this.tableWidth[1],
        dataIndex: 'physicalTableRegex',
        key: 'physicalTableRegex',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          'outputTopic'
        ),
        width: this.tableWidth[2],
        dataIndex: 'outputTopic',
        key: 'outputTopic',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          'incompatibleColumn'
        ),
        width: this.tableWidth[2],
        dataIndex: 'incompatibleColumn',
        key: 'incompatibleColumn',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          'ignoreColumn'
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
        title={'添加Table'}
      >
        <Form>
          <Row>
            <Col span={8}>
              <FormItem  label={'dataSource'} {...formItemLayout}>
                <Input
                  readOnly
                  size="small"
                  type="text"
                  value={`${record.ds_type}/${record.ds_name}`}
                />
              </FormItem>
            </Col>
            <Col span={8}>
              <FormItem label={'Schema'} {...formItemLayout}>
                <Input
                  readOnly
                  size="small"
                  type="text"
                  value={record.schema_name}
                />
              </FormItem>
            </Col>
            <Col span={8}>
              <FormItem label={'Description'} {...formItemLayout}>
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
              <FormItem label={'status'} {...formItemLayout}>
                <Input
                  readOnly
                  size="small"
                  type="text"
                  value={record.status}
                />
              </FormItem>
            </Col>
            <Col span={8}>
              <FormItem label={'src_topic'} {...formItemLayout}>
                <Input
                  readOnly
                  size="small"
                  type="text"
                  value={record.src_topic}
                />
              </FormItem>
            </Col>
            <Col span={8}>
              <FormItem label={'target_topic'} {...formItemLayout}>
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
