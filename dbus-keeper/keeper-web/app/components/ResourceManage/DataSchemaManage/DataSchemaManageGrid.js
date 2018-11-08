import React, { PropTypes, Component } from 'react'
import { Popconfirm, Tooltip, Form, Select, Input, message,Table ,Tag} from 'antd'
import { FormattedMessage } from 'react-intl'
import OperatingButton from '@/app/components/common/OperatingButton'

// 导入样式
import styles from './res/styles/index.less'
import Request from "@/app/utils/request";

const FormItem = Form.Item
const Option = Select.Option

export default class DataSchemaManageGrid extends Component {
  constructor (props) {
    super(props)
    this.state = {
    }
    this.tableWidth = [
      '4%',
      '8%',
      '10%',
      '10%',
      '10%',
      '15%',
      '13%',
      '150px'
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
    <Tooltip title={text}>
      <div className={styles.ellipsis}>
        {text}
      </div>
    </Tooltip>
  )

  renderStatus =(text, record, index) => {
    let color
    switch (text) {
      case 'active':
        color = 'green'
        break
      default:
        color = '#929292'
    }
    return (<div title={text} className={styles.ellipsis}>
      <Tag color={color} style={{cursor: 'auto'}}>
        {text}
      </Tag>
    </div>)
  }

  /**
   * @description option render
   */
  renderOperating = (text, record, index) => {
    const {onRerun, onModify, onAdd} = this.props
    return (
      <div>
        <OperatingButton icon="plus" onClick={() => onAdd(record)}>
          <FormattedMessage
            id="app.components.resourceManage.dataSchema.addTable"
            defaultMessage="添加表"
          />
        </OperatingButton>
        <OperatingButton icon="edit" onClick={() => onModify(record)}>
          <FormattedMessage id="app.common.modify" defaultMessage="修改" />
        </OperatingButton>
        <OperatingButton disabled={record.ds_type === 'db2'} icon="reload" onClick={() => onRerun(record)}>
          <FormattedMessage
            id="app.components.projectManage.projectTopology.table.rerun"
            defaultMessage="拖回重跑"
          />
        </OperatingButton>
        <Popconfirm title={<div><FormattedMessage id="app.common.delete" defaultMessage="删除" />？</div>} onConfirm={() => this.handleDelete(record)} okText="Yes" cancelText="No">
          <OperatingButton icon="delete">
            <FormattedMessage id="app.common.delete" defaultMessage="删除" />
          </OperatingButton>
        </Popconfirm>
      </div>
    )
  }

  handleDelete = (record) => {
    const {deleteApi, onRefresh} = this.props
    Request(`${deleteApi}/${record.id}`, {
      method: 'get'
    })
      .then(res => {
        if (res && res.status === 0) {
          onRefresh()
          message.success(res.message)
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

  render () {

    const {
      dataSchemaList,
      onPagination,
      onShowSizeChange
    } = this.props
    const { loading, loaded } = dataSchemaList
    const { total, pageSize, pageNum, list } = dataSchemaList.result
    const dataSchema = dataSchemaList.result && dataSchemaList.result.list
    const columns = [
      {
        title: 'ID',
        width: this.tableWidth[0],
        dataIndex: 'id',
        key: 'id',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.resourceManage.dataSourceName"
            defaultMessage="数据源名称"
          />
        ),
        width: this.tableWidth[1],
        dataIndex: 'ds_name',
        key: 'ds_name',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.resourceManage.dataSchemaName"
            defaultMessage="Schema名称"
          />
        ),
        width: this.tableWidth[2],
        dataIndex: 'schema_name',
        key: 'schema_name',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.common.status"
            defaultMessage="状态"
          />
        ),
        width: this.tableWidth[3],
        dataIndex: 'status',
        key: 'status',
        render: this.renderComponent(this.renderStatus)
      },
      {
        title: (
          <FormattedMessage
            id="app.common.updateTime"
            defaultMessage="更新时间"
          />
        ),
        width: this.tableWidth[5],
        dataIndex: 'create_time',
        key: 'create_time',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.common.description"
            defaultMessage="描述"
          />
        ),
        width: this.tableWidth[6],
        dataIndex: 'description',
        key: 'description',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.resourceManage.sourceTopic"
            defaultMessage="源Topic"
          />
        ),
        width: this.tableWidth[6],
        dataIndex: 'src_topic',
        key: 'src_topic',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.resourceManage.targetTopic"
            defaultMessage="目标Topic"
          />
        ),
        width: this.tableWidth[6],
        dataIndex: 'target_topic',
        key: 'target_topic',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.common.operate"
            defaultMessage="操作"
          />
        ),
        width: this.tableWidth[7],
        key: 'operate',
        render: this.renderComponent(this.renderOperating)
      }
    ]
    const pagination = {
      showSizeChanger: true,
      showQuickJumper: true,
      pageSizeOptions: ['10', '20', '50', '100'],
      current: pageNum || 1,
      pageSize: pageSize || 10,
      total: total,
      onChange: onPagination,
      onShowSizeChange: onShowSizeChange
    }
    return (
      <div className={styles.table}>
        <Table
          rowKey={record => record.id}
          dataSource={dataSchema}
          columns={columns}
          pagination={pagination}
        />
      </div>
    )
  }
}

DataSchemaManageGrid.propTypes = {
}
