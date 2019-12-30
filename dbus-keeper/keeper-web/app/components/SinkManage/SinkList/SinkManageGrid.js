import OperatingButton from '@/app/components/common/OperatingButton'
import React, {Component, PropTypes} from 'react'
import {message, Popconfirm, Table, Tooltip} from 'antd'
import {FormattedMessage} from 'react-intl'
import {DELETE_SINK_API} from '@/app/containers/SinkManage/api'
// 导入样式
import styles from './res/styles/index.less'
import Request from '@/app/utils/request'

export default class SinkManageGrid extends Component {
  componentWillMount () {
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
  /**
   * @param page  传入的跳转页码  type:[Object Number]
   * @description table分页
   */
  handlePagation = page => {
    const { onPagination } = this.props
    onPagination(page)
  };

  handleDelete = id => {
    const {onSearch, sinkParams} = this.props
    const requestAPI = `${DELETE_SINK_API}/${id}`
    Request(requestAPI)
      .then(res => {
        if (res && res.status === 0) {
          onSearch(sinkParams)
        } else {
          message.warn(res.message)
        }
      })
      .catch(error => {
        error.response.data && error.response.data.message
          ? message.error(error.response.data.message)
          : message.error(error.message)
        this.setState({loading: false})
      })
  }

  handleModify = record => {
    const {
      onModify
    } = this.props
    onModify(Object.assign({},record))
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
      <OperatingButton onClick={() => this.handleModify(record)} icon="edit">
        <FormattedMessage id="app.common.modify" defaultMessage="修改" />
      </OperatingButton>
      <Popconfirm placement="bottom" title={<span>
        <FormattedMessage id="app.common.delete" defaultMessage="删除" />?
      </span>} onConfirm={() => this.handleDelete(record.id)} okText="Yes" cancelText="No">
        <OperatingButton icon="delete">
          <FormattedMessage id="app.common.delete" defaultMessage="删除" />
        </OperatingButton>
      </Popconfirm>
      <OperatingButton onClick={() => this.props.onMount(record)} icon="fork">
        <FormattedMessage
          id="app.components.resourceManage.dataSource.viewMountProject"
          defaultMessage="查看已挂载项目"
        />
      </OperatingButton>
    </div>
  );
  render () {
    const {
      sinkList,
      tableWidth,
    } = this.props
    const list = sinkList && sinkList.list
    const pagination = {
      showQuickJumper: true,
      current: (sinkList && sinkList.pageNum) || 1,
      pageSize: (sinkList && sinkList.pageSize) || 10,
      total: sinkList && sinkList.total,
      onChange: this.handlePagation
    }
    const columns = [
      {
        title: <FormattedMessage
          id="app.components.sinkManage.sinkName"
          defaultMessage="Sink名称"
        />,
        width: tableWidth[0],
        dataIndex: 'sinkName',
        key: 'sinkName',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: <FormattedMessage
          id="app.components.sinkManage.bootstrapServers"
          defaultMessage="Kafka服务器"
        />,
        width: tableWidth[1],
        dataIndex: 'url',
        key: 'url',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: <FormattedMessage
          id="app.common.version"
          defaultMessage="版本"
        />,
        width: tableWidth[2],
        dataIndex: 'sinkType',
        key: 'sinkType',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: <FormattedMessage
          id="app.common.description"
          defaultMessage="描述"
        />,
        width: tableWidth[3],
        dataIndex: 'sinkDesc',
        key: 'sinkDesc',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage id="app.common.operate" defaultMessage="操作" />
        ),
        width: tableWidth[4],
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
        />
      </div>
    )
  }
}

SinkManageGrid.propTypes = {
  tableWidth: PropTypes.array,
  locale: PropTypes.any,
  resourceList: PropTypes.object,
  onPagination: PropTypes.func,
  onShowSizeChange: PropTypes.func
}
