import OperatingButton from '@/app/components/common/OperatingButton'
import React, {Component, PropTypes} from 'react'
import {message, Popconfirm, Table, Tooltip} from 'antd'
import {FormattedMessage} from 'react-intl'
// 导入样式
import styles from './res/styles/index.less'
import Request from "@/app/utils/request";
import {DELETE_SINKER_SCHEMA_API} from "@/app/containers/SinkManage/api";

export default class SinkerSchemaGrid extends Component {
  componentWillMount() {
  }

  /**
   * @param key 传入一个key type:[Object String]  默认:空
   * @returns 返回一个随机字符串
   */
  handleRandom = key =>
    `${Math.random()
      .toString(32)
      .substr(3, 8)}${key || ''}`;
  /**
   * @param page  传入的跳转页码  type:[Object Number]
   * @description table分页
   */
  handlePagation = page => {
    const {onPagination} = this.props
    onPagination(page)
  };

  handleModify = record => {
    const {onModify} = this.props
    onModify(record)
  }

  handleAddTable = record => {
    const {onAddTable} = this.props
    onAddTable(record)
  }

  handleDelete = record => {
    Request(`${DELETE_SINKER_SCHEMA_API}/${record.id}`, {
      method: 'get'
    })
      .then(res => {
        if (res && res.status === 0) {
          message.success(res.message)
          const {onSearch, searchParams} = this.props
          onSearch(searchParams)
        } else {
          message.warn(res.message)
        }
      })
      .catch(error => {
        error.response.data && error.response.data.message
          ? message.error(error.response.data.message)
          : message.error(error.message)
      })
  }
  /**
   * @param render 传入一个render
   * @returns render 返回一个新的render
   * @description 统一处理render函数
   */
  renderComponent = render => (text, record, index) =>
    render(text, record, index);

  /**
   * @description table默认的render
   */
  renderNomal = (text, record, index) => (
    <Tooltip title={text}>
      <div className={styles.ellipsis}>
        {text}
      </div>
    </Tooltip>
  )
  /**
   * @description selectTable的 option render
   */
  renderOperating = (text, record, index) => (
    <div>
      <OperatingButton icon="plus" onClick={() => this.handleAddTable(record)}>
        添加表
      </OperatingButton>
      <OperatingButton onClick={() => this.handleModify(record)} icon="edit">
        <FormattedMessage id="app.common.modify" defaultMessage="修改"/>
      </OperatingButton>
      <Popconfirm placement="bottom" title={<span>
        <FormattedMessage id="app.common.delete" defaultMessage="删除"/>?
      </span>} onConfirm={() => this.handleDelete(record)} okText="Yes" cancelText="No">
        <OperatingButton icon="delete">
          <FormattedMessage id="app.common.delete" defaultMessage="删除"/>
        </OperatingButton>
      </Popconfirm>
    </div>
  );

  render() {
    const {
      sinkerSchemaList,
      tableWidth,
      selectedRowKeys,
      onSelectionChange,
      onShowSizeChange
    } = this.props
    const list = sinkerSchemaList && sinkerSchemaList.list
    const pagination = {
      showQuickJumper: true,
      showSizeChanger: true,
      pageSizeOptions: ['10', '20', '50', '100'],
      current: (sinkerSchemaList && sinkerSchemaList.pageNum) || 1,
      pageSize: (sinkerSchemaList && sinkerSchemaList.pageSize) || 10,
      total: sinkerSchemaList && sinkerSchemaList.total,
      onChange: this.handlePagation,
      onShowSizeChange: onShowSizeChange
    }
    const columns = [
      {
        title: 'ID',
        width: tableWidth[0],
        dataIndex: 'id',
        key: 'id',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: 'Sinker名称',
        width: tableWidth[1],
        dataIndex: 'sinkerName',
        key: 'sinkerName',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: '数据源名称',
        width: tableWidth[2],
        dataIndex: 'dsName',
        key: 'dsName',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: 'Schema名称',
        width: tableWidth[3],
        dataIndex: 'schemaName',
        key: 'schemaName',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: '源Topic',
        width: tableWidth[4],
        dataIndex: 'targetTopic',
        key: 'targetTopic',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: '描述',
        width: tableWidth[5],
        dataIndex: 'description',
        key: 'description',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage id="app.common.operate" defaultMessage="操作"/>
        ),
        width: '200px',
        render: this.renderComponent(this.renderOperating)
      }
    ]
    return (
      <div className={styles.table}>
        <Table
          rowKey={record => record.id}
          dataSource={list}
          columns={columns}
          pagination={pagination}
          rowSelection={{
            onChange: onSelectionChange,
            selectedRowKeys: selectedRowKeys
          }}
        />
      </div>
    )
  }
}

SinkerSchemaGrid.propTypes = {
  tableWidth: PropTypes.array,
  locale: PropTypes.any,
  resourceList: PropTypes.object,
  onPagination: PropTypes.func,
  onShowSizeChange: PropTypes.func
}
