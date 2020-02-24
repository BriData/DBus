import React, {Component} from 'react'
import {Col, Form, Input, message, Modal, Row, Table} from 'antd'
import {FormattedMessage} from 'react-intl'
// 导入样式
import styles from './res/styles/index.less'
import Request from '@/app/utils/request'
import {ADD_SINKER_TABLES_API} from '@/app/containers/SinkManage/api'

const FormItem = Form.Item

@Form.create()
export default class SinkerSchemaAddTableModal extends Component {
  constructor(props) {
    super(props)
  }

  componentWillMount() {
    const {onSearchTableList, record} = this.props
    onSearchTableList(record)
  }

  /**
   * @param render 传入一个render
   * @returns render 返回一个新的render
   * @description 统一处理render函数
   */
  renderComponent = render => (text, record, index) =>
    render(text, record, index)

  /**
   * @description 默认的render
   */
  renderNomal = (text, record, index) => (
    <div title={text} className={styles.ellipsis}>
      {text}
    </div>
  )

  handleSubmit = () => {
    const {onClose, record} = this.props
    const {selectedRows, onSetSelectRows} = this.props
    Request(ADD_SINKER_TABLES_API, {
      data: {
        sinkerSchema: record,
        sinkerTableList: selectedRows
      },
      method: 'post'
    })
      .then(res => {
        if (res && res.status === 0) {
          message.success(res.message)
          onSetSelectRows([])
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
    const {getFieldDecorator} = this.props.form
    const {visible, key, record, onClose, sinkerTableList, onSetSelectRows} = this.props
    const formItemLayout = {
      labelCol: {
        span: 7
      },
      wrapperCol: {
        span: 10
      },
      style: {
        marginBottom: 0
      }
    }

    const columns = [
      {
        title: 'ID',
        width: '5%',
        dataIndex: 'id',
        key: 'id',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: '表ID',
        width: '5%',
        dataIndex: 'tableId',
        key: 'tableId',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: '表名',
        width: '10%',
        dataIndex: 'tableName',
        key: 'tableName',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: '描述',
        width: '10%',
        dataIndex: 'description',
        key: 'description',
        render: this.renderComponent(this.renderNomal)
      }
    ]

    const rowSelection = {
      onChange: (selectedRowKeys, selectedRows) => {
        onSetSelectRows(selectedRows)
      },
      getCheckboxProps: item => ({
        disabled: item.sinkerTopoId != null,
        defaultChecked: item.sinkerTopoId != null,
      })
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
            id="app.common.addTable"
            defaultMessage="添加表"
          />
        </div>
        }
      >
        <div className="form-search">
          <Form>
            <Row>
              <Col span={8}>
                <FormItem label='数据源名称'{...formItemLayout}>
                  <Input
                    readOnly
                    size="small"
                    type="text"
                    value={record && record.dsName}
                    disabled={true}
                  />
                </FormItem>
              </Col>
              <Col span={8}>
                <FormItem label='Schema名称'{...formItemLayout}>
                  <Input
                    readOnly
                    size="small"
                    type="text"
                    value={record && record.schemaName}
                    disabled={true}
                  />
                </FormItem>
              </Col>
              <Col span={8}>
                <FormItem label='sinkerName'{...formItemLayout}>
                  <Input
                    readOnly
                    size="small"
                    type="text"
                    value={record && record.sinkerName}
                    disabled={true}
                  />
                </FormItem>
              </Col>
            </Row>
          </Form>
        </div>
        <Table
          rowKey={record => record.tableId}
          rowSelection={rowSelection}
          columns={columns}
          dataSource={sinkerTableList}
          pagination={false}
          scroll={{x: 800, y: 350}}
        />
      </Modal>
    )
  }
}

SinkerSchemaAddTableModal.propTypes = {}
