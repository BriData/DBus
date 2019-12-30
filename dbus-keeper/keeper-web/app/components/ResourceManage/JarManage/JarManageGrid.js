import React, {Component} from 'react'
import {Form, Popconfirm, Select, Table, Tooltip} from 'antd'
import {FormattedMessage} from 'react-intl'
import OperatingButton from '@/app/components/common/OperatingButton'
// 导入样式
import styles from './res/styles/index.less'

const FormItem = Form.Item
const Option = Select.Option

export default class JarManageGrid extends Component {
  constructor(props) {
    super(props)
    this.state = {
      versionVisible: false,
      typeVisible: false
    }
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
    const {onDelete, onModify} = this.props
    return (
      <div>
        <Popconfirm title={'确认删除？'} onConfirm={() => onDelete(record.id)} okText="Yes" cancelText="No">
          <OperatingButton icon="delete">
            <FormattedMessage id="app.common.delete" defaultMessage="删除"/>
          </OperatingButton>
        </Popconfirm>
        <OperatingButton icon="edit" onClick={() => onModify(record)}>
          <FormattedMessage id="app.common.modify" defaultMessage="编辑"/>
        </OperatingButton>
      </div>
    )
  };

  /**
   * @param state [object String] state
   * @description 控制自定义过滤弹出框的显示和隐藏
   */
  filterVisible = state => ({
    filterDropdownVisible: this.state[`${state}Visible`],
    onFilterDropdownVisibleChange: visible => {
      let filterDropdownVisible = {}
      filterDropdownVisible[`${state}Visible`] = visible
      this.setState(filterDropdownVisible)
    }
  });

  render() {
    const {tableWidth, jarInfos, onSelectChange} = this.props
    const columns = [
      {
        title: <FormattedMessage
          id="app.common.id"
          defaultMessage="ID"
        />,
        width: tableWidth[1],
        dataIndex: 'id',
        key: 'id',
        render: this.renderComponent(this.renderNomal),
      },
      {
        title: <FormattedMessage
          id="app.components.resourceManage.jarManager.category"
          defaultMessage="Category"
        />,
        width: tableWidth[2],
        dataIndex: 'category',
        key: 'category',
        render: this.renderComponent(this.renderNomal),
      },
      {
        title: <FormattedMessage
          id="app.common.version"
          defaultMessage="版本"
        />,
        width: tableWidth[3],
        dataIndex: 'version',
        key: 'version',
        render: this.renderComponent(this.renderNomal),
      },
      {
        title: <FormattedMessage
          id="app.components.resourceManage.jarManager.type"
          defaultMessage="Type"
        />,
        width: tableWidth[4],
        dataIndex: 'type',
        key: 'type',
        render: this.renderComponent(this.renderNomal),
      },
      {
        title: <FormattedMessage
          id="app.components.resourceManage.jarManager.name"
          defaultMessage="Jar包名称"
        />,
        width: tableWidth[5],
        dataIndex: 'name',
        key: 'name',
        render: this.renderComponent(this.renderNomal),
      },
      {
        title: <FormattedMessage
          id="app.components.resourceManage.jarManager.minorVersion"
          defaultMessage="小版本"
        />,
        width: tableWidth[6],
        dataIndex: 'minorVersion',
        key: 'minorVersion',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: <FormattedMessage
          id="app.components.resourceManage.jarManager.path"
          defaultMessage="Jar包路径"
        />,
        width: tableWidth[7],
        dataIndex: 'path',
        key: 'path',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: <FormattedMessage
          id="app.common.description"
          defaultMessage="描述"
        />,
        width: tableWidth[8],
        dataIndex: 'description',
        key: 'description',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage id="app.common.operate" defaultMessage="操作"/>
        ),
        width: tableWidth[0],
        key: 'operate',
        render: this.renderComponent(this.renderOperating)
      }
    ]
    const rowSelection = {
      onChange: onSelectChange
    }
    return (
      <div className={styles.table}>
        <Table
          rowKey={record => record.path}
          rowSelection={rowSelection}
          dataSource={jarInfos.result.payload}
          columns={columns}
        />
      </div>
    )
  }
}

JarManageGrid.propTypes = {}
