/**
 * @author 戎晓伟
 * @description  Sink信息设置
 */

import React, { PropTypes, Component } from 'react'
import { Tooltip, Table, Button, message, Popconfirm } from 'antd'
import { FormattedMessage } from 'react-intl'
import OperatingButton from '@/app/components/common/OperatingButton'
import Request from '@/app/utils/request'
// 导入API
import {
  DELETE_USER_API
} from '@/app/containers/UserManage/api'
// 导入样式
import styles from './res/styles/index.less'

export default class UserManageGrid extends Component {
  componentWillMount () {
    // 初始化查询
    // this.handleSearch(this.initParams, true)
  }
  /**
   * @param id 用户ID
   * @description 删除用户
  */
  handleDelUser=(id) => {
    const {onSearch, userListParams} = this.props
    const requestAPI = `${DELETE_USER_API}/${id}`
    Request(requestAPI)
      .then(res => {
        if (res && res.status === 0) {
          onSearch(userListParams, false)
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
      <div className={styles.ellipsis}>
        {text}
      </div>
    </Tooltip>
  )
  /**
   * @description table PhoneNum render
  */
  renderPhoneNum=(text, record, index) => (
    <div title={text} className={styles.ellipsis}>
      {text || '--------'}
    </div>
  );
  /**
   * @description table option render
   */
  renderOperating = (text, record, index) => (
    <div>
      <OperatingButton onClick={() => this.props.onGetUserInfo(record.id)} icon="edit">
        <FormattedMessage id="app.common.modify" defaultMessage="修改" />
      </OperatingButton>
      <OperatingButton onClick={() => this.props.onSearchProject({userId: record.id, roleType: record.roleType})} icon="fork">
        <FormattedMessage
          id="app.components.userManage.viewAllocationProject"
          defaultMessage="查看已分配项目"
        />
      </OperatingButton>
      <Popconfirm placement="bottom" title={<span>
        <FormattedMessage id="app.common.delete" defaultMessage="删除" />?
      </span>} onConfirm={() => this.handleDelUser(record.id)} okText="Yes" cancelText="No">
        <OperatingButton icon="delete">
          <FormattedMessage id="app.common.delete" defaultMessage="删除" />
        </OperatingButton>
      </Popconfirm>
    </div>
  );
  render () {
    const {
      tableWidth,
      userList,
      onPagination,
      onShowSizeChange
    } = this.props
    const { loading } = userList
    const { total, pageSize, pageNum, list } = userList.result
    const dataSource = list || []
    // ID 所属项目 是否投产 数据源类型 数据源名称 Schema TableName Status Version Description CreateTime 拉全量  脱敏要求 操作
    const columns = [
      {
        title: (
          <FormattedMessage id="app.common.user.name" defaultMessage="姓名" />
        ),
        width: tableWidth[0],
        dataIndex: 'userName',
        key: 'userName',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage id="app.common.user.email" defaultMessage="邮箱" />
        ),
        width: tableWidth[1],
        dataIndex: 'email',
        key: 'email',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: <FormattedMessage
          id="app.components.userManage.userType"
          defaultMessage="用户类型"
        />,
        width: tableWidth[2],
        dataIndex: 'roleType',
        key: 'roleType',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.common.user.phone"
            defaultMessage="手机号"
          />
        ),
        width: tableWidth[3],
        dataIndex: 'phoneNum',
        key: 'phoneNum',
        render: this.renderComponent(this.renderPhoneNum)
      },
      {
        title: (
          <FormattedMessage id="app.common.operate" defaultMessage="操作" />
        ),
        width: tableWidth[4],
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
          dataSource={dataSource}
          columns={columns}
          pagination={pagination}
          loading={loading}
        />
      </div>
    )
  }
}

UserManageGrid.propTypes = {
  tableWidth: PropTypes.array,
  locale: PropTypes.any,
  userList: PropTypes.object,
  onPagination: PropTypes.func,
  onShowSizeChange: PropTypes.func,
  onSearch: PropTypes.func,
  onGetUserInfo: PropTypes.func,
  onSearchProject: PropTypes.func,
  userListParams: PropTypes.object
}
