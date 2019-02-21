import React, { PropTypes, Component } from 'react'
import { Tooltip ,Tag,Form, Select, Input, message, Table } from 'antd'
import { FormattedMessage } from 'react-intl'
import OperatingButton from '@/app/components/common/OperatingButton'

// 导入样式
import styles from './res/styles/index.less'
import Request from "@/app/utils/request";

const FormItem = Form.Item
const Option = Select.Option

export default class DataSourceManageGrid extends Component {

  constructor (props) {
    super(props)
    this.state = {
    }
    this.tableWidth = [
      '5%',
      '10%',
      '7%',
      '10%',
      '15%',
      '12%',
      '20%',
      '200px'
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

  /**
   * @description option render
   */
  renderOperating = (text, record, index) => {
    const {onRerun, onClearFullPullAlarm, onModify, onMount, onTopo, onAdd, onDBusData} = this.props
    let menus = []
    menus.push({
      text: <FormattedMessage
        id="app.components.projectManage.projectTopology.table.rerun"
        defaultMessage="拖回重跑"
      />,
      icon: 'reload',
      disabled: record.type === 'db2',
      onClick: () => onRerun(record),
    })
    if (record.type === 'mysql' || record.type === 'oracle' || record.type === 'mongo' || record.type === 'db2') {
      menus.push({
        text: <FormattedMessage
          id="app.components.resourceManage.dataSource.clearFullPullAlarm"
          defaultMessage="消除全量报警"
        />,
        confirmText: <span>
          <FormattedMessage
            id="app.components.resourceManage.dataSource.clearFullPullAlarm"
            defaultMessage="消除全量报警"
          />?
        </span>,
        icon: 'exclamation-circle-o',
        onClick: () => onClearFullPullAlarm(record),
      })
    }
    menus = [
      ...menus,
      {
        text: <FormattedMessage
          id="app.components.resourceManage.dataSource.viewMountProject"
          defaultMessage="查看已挂载项目"
        />,
        icon: 'fork',
        onClick: () => onMount(record),
      },
      {
        text: <FormattedMessage
          id="app.common.modify"
          defaultMessage="修改"
        />,
        icon: 'edit',
        onClick: () => onModify(record)
      }
    ]
    record.topoAvailableStatus === 'ALL_STOPPED' && menus.push({
      text: <FormattedMessage
        id="app.common.delete"
        defaultMessage="删除"
      />,
      icon: 'delete',
      onClick: () => this.handleDelete(record),
      confirmText: <div><FormattedMessage
        id="app.common.delete"
        defaultMessage="删除"
      />？</div>
    })
    return (
      <div>
        <OperatingButton disabled={record.type !== 'mysql' && record.type !== 'oracle' && record.type !== 'mongo'
        } icon="plus" onClick={() => onAdd(record)}>
          <FormattedMessage id="app.common.addSchema" defaultMessage="添加Schema" />
        </OperatingButton>
        <OperatingButton icon="share-alt" onClick={() => onTopo(record.id)}>
          <FormattedMessage
            id="app.components.menu.topologyManage"
            defaultMessage="拓扑管理"
          />
        </OperatingButton>
        <OperatingButton disabled={record.type !== 'mysql' && record.type !== 'oracle'
        } icon="bars" onClick={() => onDBusData(record)}>
          <FormattedMessage
            id="app.components.resourceManage.dataSource.viewDBusData"
            defaultMessage="查看DBus数据"
          />
        </OperatingButton>
        <OperatingButton icon="ellipsis" menus={menus} />
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

    return (
      <div className={styles.table}>
        <Table
          rowKey={record => record.id}
          dataSource={dataSource}
          columns={columns}
          pagination={pagination}
        />
      </div>
    )
  }
}

DataSourceManageGrid.propTypes = {
  onPagination: PropTypes.func,
  onShowSizeChange: PropTypes.func
}
