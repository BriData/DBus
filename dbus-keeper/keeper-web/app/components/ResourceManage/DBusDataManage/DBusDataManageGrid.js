import React, { PropTypes, Component } from 'react'
import { Tooltip,Form, Select, Input, message,Table ,Tag} from 'antd'
import { FormattedMessage } from 'react-intl'
import OperatingButton from '@/app/components/common/OperatingButton'

// 导入样式
import styles from './res/styles/index.less'
import Request from "@/app/utils/request";

const FormItem = Form.Item
const Option = Select.Option

export default class DBusDataManageGrid extends Component {
  constructor (props) {
    super(props)
    this.state = {
    }
    this.tableWidth = [
      '5%',
      '15%',
      '5%',
      '60%',
      '100px',
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

  /**
   * @description option render
   */
  renderOperating = (text, record, index) => {
    const {onAccessSource} = this.props
    return (
      <div>
        <OperatingButton icon="bars" onClick={() => onAccessSource(record)}>
          <FormattedMessage
            id="app.components.resourceManage.dbusData.sourceDataInsight"
            defaultMessage="查询源端数据"
          />
        </OperatingButton>
      </div>
    )
  }

  render () {

    const {
      sourceList
    } = this.props
    const columns = [
      {
        title: <FormattedMessage
          id="app.common.type"
          defaultMessage="类型"
        />,
        width: this.tableWidth[0],
        dataIndex: 'type',
        key: 'type',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.common.name"
            defaultMessage="名称"
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
            id="app.components.resourceManage.dbusData.isExist"
            defaultMessage="是否存在"
          />
        ),
        width: this.tableWidth[2],
        dataIndex: 'exist',
        key: 'exist',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.resourceManage.dbusData.columnInfo"
            defaultMessage="列信息"
          />
        ),
        width: this.tableWidth[3],
        dataIndex: 'column',
        key: 'column',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.common.operate"
            defaultMessage="操作"
          />
        ),
        width: this.tableWidth[4],
        key: 'operate',
        render: this.renderComponent(this.renderOperating)
      }
    ]
    return (
      <div className={styles.table}>
        <Table
          rowKey={record => record.name}
          dataSource={sourceList}
          columns={columns}
          pagination={false}
        />
      </div>
    )
  }
}

DBusDataManageGrid.propTypes = {
}
