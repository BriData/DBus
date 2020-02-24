import React, {Component} from 'react'
import {Button, Col, Form, Input, message, Modal, Row, Table} from 'antd'
import {FormattedMessage} from 'react-intl'
// 导入样式
import styles from './res/styles/index.less'
import Request from '@/app/utils/request'
import {ADD_SINKER_SCHEMAS_API} from '@/app/containers/SinkManage/api'

const FormItem = Form.Item

@Form.create()
export default class SinkerTopologyAddSchemaModal extends Component {
  constructor(props) {
    super(props)
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
    const {selectedRows, onSetSelectRows, sinkerSchemaList} = this.props
    Request(ADD_SINKER_SCHEMAS_API, {
      data: {
        sinkerTopology: record,
        sinkerSchemaList: selectedRows,
        originalSinkerSchemaList: sinkerSchemaList.filter(item => item.id != null && item.sinkerTopoId === record.id)
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
    const {visible, key, record, onClose, sinkerSchemaList, onSetSelectRows} = this.props
    const columns = [
      // {
      //   title: (
      //     <FormattedMessage
      //       id="app.components.sinkManage.sinkerTopo.id"
      //       defaultMessage="id"
      //     />
      //   ),
      //   width: '5%',
      //   dataIndex: 'id',
      //   key: 'id',
      //   render: this.renderComponent(this.renderNomal)
      // },
      // {
      //   title: (
      //     <FormattedMessage
      //       id="app.components.sinkManage.sinkerTopo.sinkerName"
      //       defaultMessage="Sinker Name"
      //     />
      //   ),
      //   width: '10%',
      //   dataIndex: 'sinkerName',
      //   key: 'sinkerName',
      //   render: this.renderComponent(this.renderNomal)
      // },
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
            id="app.components.resourceManage.dataSchemaId"
            defaultMessage="schema ID"
          />
        ),
        width: '10%',
        dataIndex: 'schemaId',
        key: 'schemaId',
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
        onSetSelectRows(selectedRows)
      },
      getCheckboxProps: item => ({
        defaultChecked: item.sinkerTopoId != null,
        disabled: item.sinkerTopoId != null
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
          添加schema / sinker名称: {record && record.sinkerName}
        </div>
        }
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
                  })(<Input className={styles.input} placeholder="Schema名称"/>)}
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

SinkerTopologyAddSchemaModal.propTypes = {}
