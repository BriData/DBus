/**
 * @author 戎晓伟
 * @description  Sink信息设置
 */

import React, { PropTypes, Component } from 'react'
import { Table } from 'antd'
import { FormattedMessage } from 'react-intl'
// 导入样式
import styles from './res/styles/index.less'

export default class SinkGrid extends Component {
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
    <div title={text ? String(text) : null} className={styles.ellipsis}>
      {text ? String(text) : null}
    </div>
  );
  render () {
    const { tableWidth, sinkList } = this.props
    const dataSource = sinkList
    const columns = [
      {
        title: '名称',
        width: tableWidth[0],
        dataIndex: 'sinkName',
        key: 'sinkName',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: 'URL',
        width: tableWidth[1],
        dataIndex: 'url',
        key: 'url',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: '描述',
        width: tableWidth[2],
        dataIndex: 'sinkDesc',
        key: 'sinkDesc',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: '是否投产',
        width: tableWidth[3],
        dataIndex: 'isGlobal',
        key: 'isGlobal',
        render: this.renderComponent(this.renderBoolean)
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

SinkGrid.propTypes = {
  tableWidth: PropTypes.array,
  locale: PropTypes.any,
  sinkList: PropTypes.array
}
