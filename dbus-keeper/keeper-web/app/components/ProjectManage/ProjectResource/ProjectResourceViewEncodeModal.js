import React, { PropTypes, Component } from 'react'
import {Modal, Row, Col, Table} from 'antd'
import { FormattedMessage } from 'react-intl'
import { intlMessage } from '@/app/i18n'
// 导入样式
import styles from './res/styles/index.less'

export default class ProjectResourceViewEncodeModal extends Component {
  constructor (props) {
    super(props)
    this.TableWidth = ['20%', '20%', '20%', '20%', '20%']
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
    <div
      title={text}
      className={styles.ellipsis}
    >
      {text}
    </div>
  )

  renderTruncate = (text, record, index) => {
    const truncate = text ? '是' : '否'
    return (
      <div
        title={truncate}
        className={styles.ellipsis}
      >
        {truncate}
      </div>
    )
  }

  render () {
    const {visible,record, encodeList, onClose} = this.props
    const columns = [
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectHome.tabs.resource.line"
            defaultMessage="列名"
          />
        ),
        width: this.TableWidth[0],
        dataIndex: 'fieldName',
        key: 'fieldName',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectHome.tabs.resource.fieldType"
            defaultMessage="类型"
          />
        ),
        width: this.TableWidth[1],
        dataIndex: 'dataType',
        key: 'dataType',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectHome.tabs.resource.encodeType"
            defaultMessage="脱敏规则"
          />
        ),
        width: this.TableWidth[2],
        dataIndex: 'encodeType',
        key: 'encodeType',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectHome.tabs.resource.encodeParam"
            defaultMessage="脱敏参数"
          />
        ),
        width: this.TableWidth[3],
        dataIndex: 'encodeParam',
        key: 'encodeParam',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectHome.tabs.resource.truncate"
            defaultMessage="截取"
          />
        ),
        width: this.TableWidth[4],
        dataIndex: 'truncate',
        key: 'truncate',
        render: this.renderComponent(this.renderTruncate)
      }
    ]
    const dataSource = encodeList.result && encodeList.result.payload
    return (
      <Modal
        width={1000}
        title="脱敏配置"
        visible={visible}
        onCancel={onClose}
        onOk={onClose}
        maskClosable={false}
      >
        <Row>
          <Col span={5}><b>DataSource:</b>{record.dsName}</Col>
          <Col span={5}><b>Schema:</b>{record.schemaName}</Col>
          <Col span={5}><b>Table:</b>{record.tableName}</Col>
        </Row>
        <br/>
        <Table
          size="small"
          rowKey="fieldName"
          dataSource={dataSource}
          columns={columns}
          pagination={false}
        />
      </Modal>
    )
  }
}

ProjectResourceViewEncodeModal.propTypes = {
  locale: PropTypes.any,
  tid: PropTypes.string,
  encodes: PropTypes.object,
  encodeList: PropTypes.object,
  encodeTypeList: PropTypes.object,
  onChange: PropTypes.func
}
