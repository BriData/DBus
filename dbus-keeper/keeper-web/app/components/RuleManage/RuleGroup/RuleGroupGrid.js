import React, { PropTypes, Component } from 'react'
import { Popconfirm, Form, Select, Input, message,Table,Switch } from 'antd'
import { FormattedMessage } from 'react-intl'
import OperatingButton from '@/app/components/common/OperatingButton'

// 导入样式
import styles from './res/styles/index.less'

const FormItem = Form.Item
const Option = Select.Option

export default class RuleGroupGrid extends Component {
  constructor (props) {
    super(props)
    this.state = {
    }
    this.tableWidth = [
      '10%',
      '10%',
      '10%',
      '10%',
      '10%',
      '150px'
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
    <div title={text} className={styles.ellipsis}>
      {text}
    </div>
  )
  /**
   * @description option render
   */
  renderOperating = (text, record, index) => {
    return (
      <div>
        <OperatingButton icon="edit" onClick={() => this.handleRename(record)}>
          <FormattedMessage
            id="app.components.resourceManage.ruleGroup.rename"
            defaultMessage="重命名"
          />
        </OperatingButton>
        <OperatingButton icon="copy" onClick={() => this.handleClone(record)}>
          <FormattedMessage
            id="app.components.resourceManage.ruleGroup.clone"
            defaultMessage="克隆"
          />
        </OperatingButton>
        <Popconfirm placement="bottom" title="确定删除？" onConfirm={() => this.handleDelete(record)} okText="Yes" cancelText="No">
          <OperatingButton icon="delete">
            <FormattedMessage
              id="app.common.delete"
              defaultMessage="删除"
            />
          </OperatingButton>
        </Popconfirm>
      </div>
    )
  }
  renderName = (text, record, index) => (
    <div title={text} className={styles.ellipsis}>
      <a
        title="编辑规则"
        onClick={() => this.handleEditRuleGroup(record)}>
        {text}
      </a>
    </div>
  )


  renderStatus = (text, record, index) => (
    <div title={text} className={styles.ellipsis}>
      <Switch
        checked={text === 'active'}
        onChange={checked => this.handleStatusChange(checked, record)}
      />
    </div>
  )

  renderSchema = (text, record, index) => {
    if (text === undefined || text === null) return <div/>
    text = JSON.parse(text)
    text = text.map(function (item) {
      return <li>{'Field ' + item.ruleScope + ': ' + item.name + '(' + item.type + ')'}</li>
    })
    return (
      <antd.Popover content={<ul>{text}</ul>} title="Schema">
        {text.saveAs.substr(0, 20) + '...'}
      </antd.Popover>
    )
  }

  handleEditRuleGroup = record => {
    const {onEditRuleGroup} = this.props
    onEditRuleGroup(record)
  }

  handleDelete = record => {
    const {deleteApi, onRequest} = this.props
    onRequest({
      api: `${deleteApi}/${record.id}`
    })
  }

  handleClone = record => {
    const {onOpenCloneModal} = this.props
    onOpenCloneModal(record)
  }

  handleRename = record => {
    const {onOpenRenameModal} = this.props
    onOpenRenameModal(record)
  }

  handleStatusChange = (checked, record) => {
    const {onRequest, updateApi, tableId} = this.props
    onRequest({
      api: updateApi,
      params: {
        tableId,
        newStatus: checked ? 'active' : 'inactive',
        newName: record.groupName,
        groupId: record.id
      }
    })
  }

  render () {
    const {onSelectChange} = this.props
    const {ruleGroups} = this.props
    const {group, saveAs} = ruleGroups.result
    const dataSource = group && group.map(g => {
      saveAs.forEach(sa => {
        sa.groupId === g.id && (g.saveAs = sa.ruleGrammar)
      })
      return g
    })
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
            id="app.components.resourceManage.ruleGroup.groupName"
            defaultMessage="规则组名"
          />
        ),
        width: this.tableWidth[1],
        dataIndex: 'groupName',
        key: 'groupName',
        render: this.renderComponent(this.renderName)
      },
      {
        title: (
          <FormattedMessage
            id="app.common.status"
            defaultMessage="状态"
          />
        ),
        width: this.tableWidth[2],
        dataIndex: 'status',
        key: 'status',
        render: this.renderComponent(this.renderStatus)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.resourceManage.ruleGroup.schema"
            defaultMessage="输出Schema"
          />
        ),
        width: this.tableWidth[3],
        dataIndex: 'schema',
        key: 'schema',
        render: this.renderComponent(this.renderSchema)
      },
      {
        title: (
          <FormattedMessage
            id="app.common.updateTime"
            defaultMessage="更新时间"
          />
        ),
        width: this.tableWidth[4],
        dataIndex: 'updateTime',
        key: 'updateTime',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.common.operate"
            defaultMessage="操作"
          />
        ),
        width: this.tableWidth[5],
        dataIndex: 'operation',
        key: 'operation',
        render: this.renderComponent(this.renderOperating)
      },
    ]
    return (
      <div className={styles.table}>
        <Table
          rowKey="id"
          rowSelection={{
            onChange: onSelectChange,
          }}
          pagination={false}
          size="default"
          dataSource={dataSource}
          columns={columns}>
        </Table>
      </div>
    )
  }
}

RuleGroupGrid.propTypes = {
}
