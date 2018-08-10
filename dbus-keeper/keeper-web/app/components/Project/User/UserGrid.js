/**
 * @author 戎晓伟
 * @description  用户信息设置
 */

import React, { PropTypes, Component } from 'react'
import { Table, Button, Input } from 'antd'
import { FormattedMessage } from 'react-intl'
import OperatingButton from '@/app/components/common/OperatingButton'
// 导入样式
import styles from './res/styles/index.less'

export default class UserGrid extends Component {
  componentWillMount () {
    // 初始化查询
    // this.handleSearch(this.initParams, true)
  }

  handleModify = (id) => {
    const {
      onModify
    } = this.props
    onModify(id)
  };

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
    <div title={text} className={styles.ellipsis}>
      {text}
    </div>
  );

  /**
   * @description true,false
   */
  renderBoolean = (text, record, index) => (
    <div title={String(text)} className={styles.ellipsis}>
      {String(text)}
    </div>
  );
  /**
   * @description 1,0
   */
  renderOneZero = (text, record, index) => (
    <div title={text === 1 ? 'yes' : 'no'} className={styles.ellipsis}>
      {text === 1 ? 'yes' : 'no'}
    </div>
  );
  /**
   * @description selectTable的 option render
   */
  renderOperating = (text, record, index) => (
    <div>
      <OperatingButton icon="delete" onClick={() => this.handleModify(record.projectId)}>
        {'删除项目用户'}
      </OperatingButton>
    </div>
  );
  render () {
    const {
      tableWidth,
      userList,
    } = this.props
    const dataSource = userList
    const columns = [
      {
        title: '姓名',
        width: tableWidth[0],
        dataIndex: 'userName',
        key: 'userName',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: '邮箱',
        width: tableWidth[1],
        dataIndex: 'email',
        key: 'email',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: '手机号',
        width: tableWidth[2],
        dataIndex: 'phoneNum',
        key: 'phoneNum',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: '备注',
        width: tableWidth[3],
        dataIndex: 'nodes',
        key: 'nodes',
        render: this.renderComponent(this.renderNomal)
      }
    ]

    return (
      <div className={styles.table}>
        <Table
          rowKey={record => `${record.id}`}
          dataSource={dataSource}
          columns={columns}
          pagination={false}
        />
      </div>
    )
  }
}

UserGrid.propTypes = {
  tableWidth: PropTypes.array,
  locale: PropTypes.any,
  userList: PropTypes.array,
  onPagination: PropTypes.func,
  onShowSizeChange: PropTypes.func
}
