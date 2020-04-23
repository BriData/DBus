/**
 * @author Hongchun Yin
 * @description  全量拉取历史列表
 */

import React, {Component, PropTypes} from 'react'
import {message, Popconfirm, Table, Tag, Tooltip} from 'antd'
import {FormattedMessage} from 'react-intl'
// 导入样式
import styles from './res/styles/index.less'
import Request from '@/app/utils/request'
import {DATA_RERUN_API, DATA_RESUMING_FULLPULL_API} from '@/app/containers/ResourceManage/api'
import OperatingButton from '@/app/components/common/OperatingButton'

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
      .substr(3, 8)}${key || ''}`

  // table render
  /**
   * @param render 传入一个render
   * @returns render 返回一个新的render
   * @description 统一处理render函数
   */
  renderComponent = render => (text, record, index) =>
    render(text, record, index)

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

  handleResumeFullpull = (record) => {
    Request(`${DATA_RESUMING_FULLPULL_API}/${record.id}`, {
      method: 'get'
    })
      .then(res => {
        if (res && res.status === 0) {
          message.success('全量任务断点续传成功.')
        } else {
          message.warn(res.message)
        }
      })
      .catch(error => message.error(error))
  }

  handleRerunFullpull = (record) => {
    Request(`${DATA_RERUN_API}/${record.id}`, {
      method: 'get'
    })
      .then(res => {
        if (res && res.status === 0) {
          message.success('全量任务重跑成功.')
        } else {
          message.warn(res.message)
        }
      })
      .catch(error => message.error(error))
  }

  renderOperating = (text, record, index) => {
    const {onModify} = this.props
    return (
      <div className={styles.ellipsis} style={{minWidth: 100}}>
        <OperatingButton icon="edit" onClick={() => onModify(record)}>
          <FormattedMessage id="app.common.modify" defaultMessage="修改"/>
        </OperatingButton>
        <Popconfirm placement="bottom" title="确定断点续传？" onConfirm={() => this.handleResumeFullpull(record)} okText="Yes"
                    cancelText="No">
          <OperatingButton icon='link'>
            <FormattedMessage
              id='app.components.projectManage.projectFullpullHistory.table.resumeFromBreakPoint'
              defaultMessage='断点续传'
            />
          </OperatingButton>
        </Popconfirm>
        <Popconfirm placement="bottom" title="确定任务重跑？" onConfirm={() => this.handleRerunFullpull(record)} okText="Yes"
                    cancelText="No">
          <OperatingButton icon='to-top'>
            <FormattedMessage
              id='app.components.projectManage.projectFullpullHistory.table.rerun'
              defaultMessage='任务重跑'
            />
          </OperatingButton>
        </Popconfirm>
      </div>
    )
  }

  renderStatus = (text, record, index) => {
    let color
    switch (text) {
      case 'init':
        color = 'DarkSeaGreen'
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
    return this.renderNomal(`${record.dsName}.${record.schemaName}.${record.tableName}`, record, index)
  }

  renderTopoTable = (text, record, index) => {
    const projectName = record.projectDisplayName === null ? ' ' : record.projectDisplayName
    const topologyTableId = record.topologyTableId === null ? ' ' : record.topologyTableId
    let topoTable = `${topologyTableId}/${projectName}`
    return (
      <Tooltip title={text}>
        <div className={styles.ellipsis} style={{minWidth: 150}}>
          {topoTable}
        </div>
      </Tooltip>
    )
  }

  renderOffset = (text, record, index) => {
    const firstShardOffset = record.firstShardMsgOffset === null ? ' ' : record.firstShardMsgOffset
    const lastShardOffset = record.lastShardMsgOffset === null ? ' ' : record.lastShardMsgOffset
    const currentShardOffset = record.currentShardOffset === null ? ' ' : record.currentShardOffset
    return this.renderNomal(`${firstShardOffset} /${lastShardOffset} /${currentShardOffset}`, record, index)
  }

  /**
   * @description 全量拉取历史 目标sink
   */
  renderSinkInfo = (text, record, index) => {
    if (record.targetSinkTopic) {
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
  }

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

  renderEndTime = (text, record, index) => {
    const state = record.state
    if (text) {
      return this.renderTime(text, record, index)
    } else if (state === 'ending' || state === 'abort') {
      return this.renderTime(record.updateTime, record, index)
    }
  }

  render() {
    const {
      tableWidth,
      fullpullList,
      onPagination,
      onShowSizeChange
    } = this.props

    const {loading, loaded} = fullpullList
    const {total, pageSize, pageNum, list} = fullpullList.result
    const dataSource = list

    const columns = [
      {
        title: (
          'ID'
        ),
        width: tableWidth[0],
        dataIndex: 'id',
        key: 'id',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage id="app.common.operate" defaultMessage="操作"/>
        ),
        width: tableWidth[1],
        key: 'operation',
        render: this.renderComponent(this.renderOperating)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectFullpullHistory.table.ns"
            defaultMessage="源表信息"
          />
        ),
        width: tableWidth[5],
        render: this.renderComponent(this.renderSrcNs)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectFullpullHistory.table.topoTable"
            defaultMessage="拓扑表Id/项目名称"
          />
        ),
        width: tableWidth[4],
        dataIndex: 'topologyTableId',
        key: 'topologyTableId',
        render: this.renderComponent(this.renderTopoTable)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectFullpullHistory.table.sinkInfo"
            defaultMessage="目标信息"
          />
        ),
        width: tableWidth[6],
        render: this.renderComponent(this.renderSinkInfo)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectFullpullHistory.table.status"
            defaultMessage="状态"
          />
        ),
        width: tableWidth[7],
        dataIndex: 'state',
        key: 'state',
        render: this.renderComponent(this.renderStatus)
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
            id="app.components.projectManage.projectFullpullHistory.table.currentShardOffset"
            defaultMessage="首片Offset"
          />
        ),
        width: tableWidth[11],
        dataIndex: 'currentShardOffset',
        key: 'currentShardOffset',
        render: this.renderComponent(this.renderOffset)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectFullpullHistory.table.initTime"
            defaultMessage="初始化时间"
          />
        ),
        width: tableWidth[14],
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
        width: tableWidth[15],
        dataIndex: 'endTime',
        key: 'endTime',
        render: this.renderComponent(this.renderEndTime)
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
        width: tableWidth[2],
        dataIndex: 'type',
        key: 'type',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectFullpullHistory.table.splitColumn"
            defaultMessage="分片列"
          />
        ),
        width: tableWidth[16],
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
        width: tableWidth[17],
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
        width: tableWidth[18],
        dataIndex: 'errorMsg',
        key: 'errorMsg',
        render: this.renderComponent(this.renderNomal)
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
