import React, {PropTypes, Component} from 'react'
import {Tooltip, Tag, Form, Select, Input, message, Table} from 'antd'
import {FormattedMessage} from 'react-intl'

// 导入样式
import styles from './res/styles/index.less'


export default class BathchRestatrTopoForm extends Component {
  constructor (props) {
    super(props)
    this.state = {
    }
    this.tableWidth = [
      '5%',
      '10%',
      '10%',
      '10%',
      '15%',
      '12%',
      '20%'
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
    // renderNomal = (text, record, index) => (
    //   <div title={text} className={styles.ellipsis}>
    //     {text}
    //   </div>
    // )

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

  renderTopoStatus =(text, record, index) => {
    let color
    switch (text) {
      case 'ALL_RUNNING':
        color = 'green'
        break
      case 'PART_RUNNING':
        color = 'orange'
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

  renderOggOrCanal = (text, record, index) => {
    let title
    if (record.type === 'oracle') {
      title = <div>OGG Host：{record.oggOrCanalHost}<br/>
        OGG Path：{record.oggOrCanalPath}<br/>
        OGG Replicat Name：{record.oggReplicatName}<br/>
        OGG Trail Prefix: {record.oggTrailName}<br/>
      </div>
    } else if (record.type === 'mysql') {
      title = <div>Canal Host：{record.oggOrCanalHost}<br/>
        Canal Path：{record.oggOrCanalPath}<br/>
      </div>
    }

    return (
      <Tooltip title={title}>
        <div className={styles.ellipsis}>
          {text}
        </div>
      </Tooltip>
    )
  }


  render () {
    const {
      selectedRowKeys,
      onSelectionChange,
      dataSourceList,
      onPagination,
      onShowSizeChange
    } = this.props
    const { loading, loaded } = dataSourceList
    const { total, pageSize, pageNum, list } = dataSourceList.result
    const dataSource = dataSourceList.result && dataSourceList.result.list
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

    // ID Name Type Status TopoStatus UpdateTime Desc 操作
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
        dataIndex: 'name',
        key: 'name',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.resourceManage.dataSourceType"
            defaultMessage="数据源类型"
          />
        ),
        width: this.tableWidth[2],
        dataIndex: 'type',
        key: 'type',
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
            id="app.components.resourceManage.topoStatus"
            defaultMessage="拓扑状态"
          />
        ),
        width: this.tableWidth[4],
        dataIndex: 'topoAvailableStatus',
        key: 'topoAvailableStatus',
        render: this.renderComponent(this.renderTopoStatus)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.resourceManage.oggOrCanal"
            defaultMessage="OGG或Canal"
          />
        ),
        width: this.tableWidth[1],
        dataIndex: 'oggOrCanalHost',
        key: 'oggOrCanalHost',
        render: this.renderComponent(this.renderOggOrCanal)
      },
      {
        title: (
          <FormattedMessage
            id="app.common.updateTime"
            defaultMessage="更新时间"
          />
        ),
        width: this.tableWidth[5],
        dataIndex: 'updateTime',
        key: 'updateTime',
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
        key: 'desc',
        render: this.renderComponent(this.renderNomal)
      }
    ]

    return (
      <div className={styles.table}>
        <Table
          rowKey={record => record.id}
          dataSource={dataSource}
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

BathchRestatrTopoForm.propTypes = {
  onPagination: PropTypes.func,
  onShowSizeChange: PropTypes.func
}
