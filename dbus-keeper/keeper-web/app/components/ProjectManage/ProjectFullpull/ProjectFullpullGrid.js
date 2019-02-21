/**
 * @author Hongchun Yin
 * @description  全量拉取历史列表
 */

import React, { PropTypes, Component } from 'react'
import { Tooltip, Table,Tag , Button, Input } from 'antd'
import { FormattedMessage } from 'react-intl'
import OperatingButton from '@/app/components/common/OperatingButton'
// 导入样式
import styles from './res/styles/index.less'

export default class ProjectFullpullGrid extends Component {
  componentWillMount() {
    // 初始化查询
    // this.handleSearch(this.initParams, true)
  }

  /**
   * @param key 传入一个key type:[Object String]  默认:空
   * @returns 返回一个随机字符串
   */
  handleRandom = key =>
    `${Math.random()
      .toString(32)
      .substr(3, 8)}${key || ''}`;

  // table render
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
      <div className={styles.ellipsis} style={{minWidth: 100}}>
        {text}
      </div>
    </Tooltip>
  )

  renderOperating = (text, record, index) => {
    const {onModify} = this.props
    return (
      <div>
        <OperatingButton onClick={() => onModify(record)} icon="edit">
          <FormattedMessage id="app.common.modify" defaultMessage="修改"/>
        </OperatingButton>
      </div>
    )
  }

  renderStatus = (text, record, index) => {
    let color
    switch (text) {
      case 'init':
        color = 'pink'
        break
      case 'splitting':
      case 'pulling':
        color = 'blue'
        break
      case 'ending':
        color = 'green'
        break
      case 'abort':
        color = 'red'
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
   * @description 全量拉取历史 源表信息render
   */
  renderSrcNs = (text, record, index) => {
    return this.renderNomal(`${record.dsName}.${record.schemaName}.${record.tableName}`,record, index)
  }

  /**
   * @description 全量拉取历史 目标sink
   */
  renderSinkInfo = (text, record, index) => {
    if (record.targetSinkName && record.targetSinkTopic) {
      return this.renderNomal(`Sink名称:${record.targetSinkName},Topic:${record.targetSinkTopic}`, record, index)
    } else if (record.targetSinkTopic) {
      return this.renderNomal(record.targetSinkTopic)
    } else {
      return this.renderNomal('')
    }
  }

  /**
   * @description 全量拉取历史 已完成比例
   */
  renderCompleteRate = (colCode) => (text, record, index) => {
    var completed = colCode === 'PartitionCount' ? record.finishedPartitionCount : record.finishedRowCount
    var total = colCode === 'PartitionCount' ? record.totalPartitionCount : record.totalRowCount
    var rate = null

    if (completed != null) {
      rate = completed / total
      rate = Number(rate * 100).toFixed(0)
      rate += '%'
      rate = `${completed}/${total}(${rate})`
    }
    return this.renderNomal(rate, record, index)
  };

  /**
   * @description 全量拉取历史 时间列render
   */
  renderTime = (text, record, index) => {
    var timeInfo = null
    if (text) {
      var unixTimestamp = new Date(text)

      var month = unixTimestamp.getMonth() + 1
      month = month < 10 ? `0${month}` : month
      var dayInfo = unixTimestamp.getDate()
      dayInfo = dayInfo < 10 ? `0${dayInfo}` : dayInfo
      var hour = unixTimestamp.getHours()
      hour = hour < 10 ? `0${hour}` : hour
      var minute = unixTimestamp.getMinutes()
      minute = minute < 10 ? `0${minute}` : minute
      var seconds = unixTimestamp.getSeconds()
      seconds = seconds < 10 ? `0${seconds}` : seconds

      timeInfo = `${unixTimestamp.getFullYear()}-${month}-${dayInfo} ${hour}:${minute}:${seconds}`
    }
    return this.renderNomal(timeInfo, record, index)
  }

  render () {
    const {
      tableWidth,
      fullpullList,
      onPagination,
      onShowSizeChange
    } = this.props

    const { loading, loaded } = fullpullList
    const { total, pageSize, pageNum, list } = fullpullList.result
    const dataSource = list
    // Old: Type NameSpace State Error_Msg Init_time End_time FinishedPartitions/Total(%) FinishedRows/Total(%)
    // New: Project TopoTableId SinkInfo ReqMsgOffset FirstShardMsgOffset LastShardMsgOffset
    // Merged: Type Project TopoTableId NameSpace SinkInfo State Init_time End_time FinishedPartitions/Total(%) FinishedRows/Total(%)  ReqMsgOffset FirstShardMsgOffset LastShardMsgOffset Error_Msg

    const columns = [
      {
        title: (
          'ID'
        ),
        width: tableWidth[1],
        dataIndex: 'id',
        key: 'id',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          (
            <FormattedMessage
              id="app.components.projectManage.projectFullpullHistory.table.pullType"
              defaultMessage="拉取方式"
            />
          )
        ),
        width: tableWidth[0],
        dataIndex: 'type',
        key: 'type',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectFullpullHistory.table.project"
            defaultMessage="项目名称"
          />
        ),
        width: tableWidth[1],
        dataIndex: 'projectDisplayName',
        key: 'projectDisplayName',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectFullpullHistory.table.topoTableId"
            defaultMessage="拓扑表Id"
          />
        ),
        width: tableWidth[2],
        dataIndex: 'topologyTableId',
        key: 'topologyTableId',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectFullpullHistory.table.ns"
            defaultMessage="源表信息"
          />
        ),
        width: tableWidth[3],
        render: this.renderComponent(this.renderSrcNs)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectFullpullHistory.table.sinkInfo"
            defaultMessage="目标信息"
          />
        ),
        width: tableWidth[4],
        render: this.renderComponent(this.renderSinkInfo)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectFullpullHistory.table.status"
            defaultMessage="状态"
          />
        ),
        width: tableWidth[5],
        dataIndex: 'state',
        key: 'state',
        render: this.renderComponent(this.renderStatus)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectFullpullHistory.table.initTime"
            defaultMessage="初始化时间"
          />
        ),
        width: tableWidth[6],
        dataIndex: 'initTime',
        key: 'initTime',
        render: this.renderComponent(this.renderTime)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectFullpullHistory.table.endTime"
            defaultMessage="完成时间"
          />
        ),
        width: tableWidth[7],
        dataIndex: 'endTime',
        key: 'endTime',
        render: this.renderComponent(this.renderTime)
      },
      {
        title: this.renderNomal(
          <FormattedMessage
            id="app.components.projectManage.projectFullpullHistory.table.finishedSplitsCount"
            defaultMessage="已完成片数"
          />
        ),
        width: tableWidth[8],
        dataIndex: 'finishedPartitionCount',
        key: 'finishedPartitionCount',
        render: this.renderComponent(this.renderCompleteRate('PartitionCount'))
      },
      {
        title: this.renderNomal(
          <FormattedMessage
            id="app.components.projectManage.projectFullpullHistory.table.finishedRowsCount"
            defaultMessage="已完成行数"
          />
        ),
        width: tableWidth[9],
        dataIndex: 'finishedRowCount',
        key: 'finishedRowCount',
        render: this.renderComponent(this.renderCompleteRate('RowCount'))
      },
      {
        title: this.renderNomal(
          <FormattedMessage
            id="app.components.projectManage.projectFullpullHistory.table.pullReqMsgOffset"
            defaultMessage="拉取信息Offset"
          />
        ),
        width: tableWidth[10],
        dataIndex: 'fullPullReqMsgOffset',
        key: 'fullPullReqMsgOffset',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectFullpullHistory.table.firstSplitOffset"
            defaultMessage="首片Offset"
          />
        ),
        width: tableWidth[11],
        dataIndex: 'firstShardMsgOffset',
        key: 'firstShardMsgOffset',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectFullpullHistory.table.lastSplitOffset"
            defaultMessage="末片Offset"
          />
        ),
        width: tableWidth[12],
        dataIndex: 'lastShardMsgOffset',
        key: 'lastShardMsgOffset',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectFullpullHistory.table.splitColumn"
            defaultMessage="分片列"
          />
        ),
        width: tableWidth[12],
        dataIndex: 'splitColumn',
        key: 'splitColumn',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectFullpullHistory.table.fullpullCondition"
            defaultMessage="拉全量条件"
          />
        ),
        width: tableWidth[12],
        dataIndex: 'fullpullCondition',
        key: 'fullpullCondition',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectFullpullHistory.table.errMsg"
            defaultMessage="出错信息"
          />
        ),
        width: tableWidth[13],
        dataIndex: 'errorMsg',
        key: 'errorMsg',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage id="app.common.operate" defaultMessage="操作" />
        ),
        width: tableWidth[0],
        key: 'operation',
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
          rowKey={record => `${record.id}`}
          dataSource={dataSource}
          columns={columns}
          pagination={pagination}
          scroll={{x: true}}
          loading={loading}
        />
      </div>
    )
  }
}
ProjectFullpullGrid.propTypes = {
  tableWidth: PropTypes.array,
  locale: PropTypes.any,
  fullpullList: PropTypes.object,
  onPagination: PropTypes.func,
  onShowSizeChange: PropTypes.func
}
