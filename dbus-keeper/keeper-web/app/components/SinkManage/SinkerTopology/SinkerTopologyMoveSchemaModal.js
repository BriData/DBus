import React, {Component} from 'react'
import {Button, Col, Form, Input, message, Modal, Row, Select, Table} from 'antd'
import {FormattedMessage} from 'react-intl'
// 导入样式
import styles from './res/styles/index.less'
import Request from '@/app/utils/request'
import {UPSERT_MANY_SINKER_SCHEMA_API} from '@/app/containers/SinkManage/api'

const FormItem = Form.Item
const Option = Select.Option
const TextArea = Input.TextArea

@Form.create()
export default class SinkerTopologyMoveSchemaModal extends Component {
  constructor(props) {
    super(props)
    this.state = {
      selectedRows: []
    }
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
    if (selectedRows.length === 0) {
      message.warn('未选中任何表')
      return
    }
    Request(UPSERT_MANY_SINKER_SCHEMA_API, {
      data: {
        sinkerTopology: record,
        sinkerSchemaList: selectedRows
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

  handleSearchSchemaList = () => {
    const {record, onSearchSchemaList} = this.props
    this.props.form.validateFieldsAndScroll((err, values) => {
      if (!err) {
        console.info(values)
        const param = {
          ...values,
          id: record.id
        }
        console.info(param)
        onSearchSchemaList(param)
      }
    })
  }

  handleRandom = key =>
    `${Math.random()
      .toString(32)
      .substr(3, 8)}${key || ''}`

  render() {
    const {getFieldDecorator} = this.props.form
    const {visible, key, record, onClose, onSetSelectRows, sinkerSchemaList, driftSinkerList} = this.props
    const columns = [
      {
        title: (
          <FormattedMessage
            id="app.components.sinkManage.sinkerTopo.id"
            defaultMessage="id"
          />
        ),
        width: '5%',
        dataIndex: 'id',
        key: 'id',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.sinkManage.sinkerTopo.sinkerName"
            defaultMessage="Sinker Name"
          />
        ),
        width: '10%',
        dataIndex: 'sinkerName',
        key: 'sinkerName',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.resourceManage.dataSourceName"
            defaultMessage="dsName"
          />
        ),
        width: '10%',
        dataIndex: 'dsName',
        key: 'dsName',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.resourceManage.dataSchemaName"
            defaultMessage="schema"
          />
        ),
        width: '10%',
        dataIndex: 'schemaName',
        key: 'schemaName',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.resourceManage.targetTopic"
            defaultMessage="目标Topic"
          />
        ),
        width: '15%',
        dataIndex: 'targetTopic',
        key: 'targetTopic',
        render: this.renderComponent(this.renderNomal)
      }
    ]
    const rowSelection = {
      onChange: (selectedRowKeys, selectedRows) => {
        this.setState({
          selectedRows: selectedRows
        })
      },
      getCheckboxProps: record => ({
        defaultChecked: record.id !== null
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
        title="漂移schema"
      >
        <div className="form-search">
          <Form autoComplete="off" layout="inline" className={styles.searchForm}>
            <Row>
              <Col span={12} className={styles.formLeft}>
                <FormItem>
                  {getFieldDecorator('dsName', {
                    initialValue: null
                  })(<Input className={styles.input} placeholder="数据源名称"/>)}
                </FormItem>
                <FormItem>
                  {getFieldDecorator('schemaName', {
                    initialValue: null
                  })(<Input className={styles.input} placeholder="schema名称"/>)}
                </FormItem>
              </Col>
              <Col span={12} className={styles.formRight}>
                <FormItem>
                  <Button
                    type="primary"
                    icon="search"
                    onClick={this.handleSearchSchemaList}
                  >
                    <FormattedMessage
                      id="app.common.search"
                      defaultMessage="查询"
                    />
                  </Button>
                </FormItem>
              </Col>
            </Row>
          </Form>
        </div>
        <Table
          rowKey={record => record.schemaId}
          rowSelection={rowSelection}
          columns={columns}
          dataSource={sinkerSchemaList}
          pagination={false}
          scroll={{x: 800, y: 350}}
        />
      </Modal>
    )
  }
}

SinkerTopologyMoveSchemaModal.propTypes = {}
