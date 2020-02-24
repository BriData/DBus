import React, {PropTypes, Component} from 'react'
import {Button, Row, Col, Modal, Form, Select, Input, Spin, Table, Icon, message} from 'antd'
import {FormattedMessage} from 'react-intl'
import OperatingButton from '@/app/components/common/OperatingButton'
import AddTableOggModal from './AddTableOggModal'
// 导入样式
import styles from './res/styles/index.less'
import Request from "@/app/utils/request";

const FormItem = Form.Item
const Option = Select.Option
const TextArea = Input.TextArea

@Form.create()
export default class DataSchemaManageAddModal extends Component {
  constructor(props) {
    super(props)
    this.state = {
      selectedRows: [],

      oggModalKey: 'oggModalKey',
      oggModalVisible: false,
      oggModalContent: null
    }
    this.tableWidth = [
      '20%',
      '20%',
      '20%',
      '20%',
      '20%'
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

  handleSubmit = () => {
    const {onClose, addApi, record} = this.props
    const {selectedRows} = this.state
    const data = {
      dataSource: {
        id: record.ds_id,
        dsName:record.ds_name,
        dsType:record.ds_type
      }
    }
    if(selectedRows.length === 0) {
      message.warn("未选中任何表")
      return
    }
    data.schemaAndTables = [
      {
        schema: {
          dsId: record.ds_id,
          dsName: record.ds_name,
          dsType: record.ds_type,
          schemaName: record.schema_name,
          srcTopic: record.src_topic,
          targetTopic: record.target_topic,
        },
        tables: selectedRows.map(table => ({
          schemaName: record.schema_name,
          tableName: table.tableName,
          physicalTableRegex: table.physicalTableRegex,
          outputTopic: table.outputTopic,
        }))
      }
    ]

    Request(addApi, {
      data: data,
      method: 'post' })
      .then(res => {
        if (res && res.status === 0) {
          message.success(res.message)
          onClose()
        } else {
          message.warn(res.message)
        }
      })
      .catch(error => message.error(error))
  }

  handleRandom = key =>
    `${Math.random()
      .toString(32)
      .substr(3, 8)}${key || ''}`


  viewOgg = () => {
    const {selectedRows} = this.state
    const {schemaTableResult} = this.props
    const schemaTable = schemaTableResult.result.payload

    const schemaInfos = {
      [schemaTable.schema.schemaName] : schemaTable.schema
    }
    const tableInfos = {
      [schemaTable.schema.schemaName] : selectedRows
    }
    const recordRowKeys = {
      [schemaTable.schema.schemaName] : [
        ...selectedRows.map(row => row.tableName),
        ...schemaTable.tables.filter(table => table.disable).map(table => table.tableName)
      ]
    }

    let string = ""
    string += "-- NEW ADD ALTER:\n"
    string += "".concat(...Object.keys(schemaInfos).map(schemaName => {
      return "".concat(...tableInfos[schemaName].map(table => {
        return `ALTER TABLE ${schemaName}.${table.tableName} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;\n`
      }))
    }))

    string += "\n-- NEW ADD OGG:\n"
    string += "".concat(...Object.keys(schemaInfos).map(schemaName => {
      return "".concat(...tableInfos[schemaName].map(table => {
        let ret = `TABLE ${schemaName}.${table.tableName}`
        if (table.columnName !== '无') {
          const nameTypes = table.columnName.split(" ");
          let colsExcept = ''
          nameTypes.forEach((nameType, index) => {
            if (nameType) {
              const slashIndex = nameType.indexOf('/')
              if (index) colsExcept += ','
              colsExcept += nameType.substr(0, slashIndex)
            }
          })
          ret += `, COLSEXCEPT ( ${colsExcept} )`
        }
        ret += ';\n'
        return ret
      }))
    }))

    string += "\n-- NEW ADD MAPS:\n"
    string += "".concat(...Object.keys(schemaInfos).map(schemaName => {
      return "".concat(...tableInfos[schemaName].map(table => {
        return `MAP ${schemaName}.${table.tableName} ,TARGET ${schemaName}.${table.tableName};\n`
      }))
    }))

    string += "\n"
    string += "".concat(...Object.keys(schemaInfos).map(schemaName => {
      return "".concat(...[
        `MAP DBUS.DB_HEARTBEAT_MONITOR, TARGET DBUS.DB_HEARTBEAT_MONITOR, WHERE (SCHEMA_NAME = '${schemaName}');\n`,
        `MAP DBUS.META_SYNC_EVENT, TARGET DBUS.META_SYNC_EVENT, WHERE (TABLE_OWNER = '${schemaName}');\n`
      ])
    }))

    string += "\n------------------------------------------------------------------------------------------------------------------------------------------------------------\n"

    string += "\n-- ALTER:\n"
    string += "".concat(...Object.keys(schemaInfos).map(schemaName => {
      return "".concat(...recordRowKeys[schemaName].map(existTableName => {
        return tableInfos[schemaName].every(table => table.tableName !== existTableName) ?
          `ALTER TABLE ${schemaName}.${existTableName} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;\n` :
          ""
      }))
    }))

    string += "\n-- OGG:\n"
    string += "".concat(...Object.keys(schemaInfos).map(schemaName => {
      return "".concat(...recordRowKeys[schemaName].map(existTableName => {
        return tableInfos[schemaName].every(table => table.tableName !== existTableName) ?
          `TABLE ${schemaName}.${existTableName};\n` :
          ""
      }))
    }))

    string += "\n-- MAPS:\n"
    string += "".concat(...Object.keys(schemaInfos).map(schemaName => {
      return "".concat(...recordRowKeys[schemaName].map(existTableName => {
        return tableInfos[schemaName].every(table => table.tableName !== existTableName) ?
          `MAP ${schemaName}.${existTableName} ,TARGET ${schemaName}.${existTableName};\n` :
          ""
      }))
    }))

    string += "\n"
    string += "".concat(...Object.keys(schemaInfos).map(schemaName => {
      return "".concat(...[
        `MAP DBUS.DB_HEARTBEAT_MONITOR, TARGET DBUS.DB_HEARTBEAT_MONITOR, WHERE (SCHEMA_NAME = '${schemaName}');\n`,
        `MAP DBUS.META_SYNC_EVENT, TARGET DBUS.META_SYNC_EVENT, WHERE (TABLE_OWNER = '${schemaName}');\n`
      ])
    }))

    this.setState({
      oggModalKey: this.handleRandom('oggModalKey'),
      oggModalVisible: true,
      oggModalContent: string
    })
  }

  closeOgg = () => {
    this.setState({
      oggModalKey: this.handleRandom('oggModalKey'),
      oggModalVisible: false
    })
  }

  render() {
    const {visible, key, record, onClose, schemaTableResult} = this.props
    const loading = schemaTableResult.loading
    const schemaTable = schemaTableResult.result.payload
    let tableList = schemaTable && schemaTable.tables || []
    tableList = tableList.map(table => ({
      ...table,
      outputTopic: table.outputTopic || record.target_topic,
      physicalTableRegex: table.physicalTableRegex || table.tableName
    }))
    let schemaInfo = schemaTable && schemaTable.schema || {}
    if (schemaInfo.dsType !== 'mysql') {
      tableList = tableList.filter(table => table.tableName.indexOf('$') < 0)
    }
    tableList = tableList.filter(table => !table.disable)
    const columns = [
      {
        title: (
          <FormattedMessage
            id="app.components.resourceManage.dataTableName"
            defaultMessage="表名"
          />
        ),
        width: this.tableWidth[0],
        dataIndex: 'tableName',
        key: 'tableName',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.resourceManage.dataTableNameRegex"
            defaultMessage="表名正则"
          />
        ),
        width: this.tableWidth[1],
        dataIndex: 'physicalTableRegex',
        key: 'physicalTableRegex',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectTable.outputTopic"
            defaultMessage="输出Topic"
          />
        ),
        width: this.tableWidth[2],
        dataIndex: 'outputTopic',
        key: 'outputTopic',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.resourceManage.dataSource.incompatibleColumn"
            defaultMessage="不兼容的列"
          />
        ),
        width: this.tableWidth[2],
        dataIndex: 'incompatibleColumn',
        key: 'incompatibleColumn',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.resourceManage.dataSource.ignoreColumn"
            defaultMessage="忽略的列"
          />
        ),
        width: this.tableWidth[2],
        dataIndex: 'columnName',
        key: 'columnName',
        render: this.renderComponent(this.renderNomal)
      }
    ]
    const rowSelection = {
      onChange: (selectedRowKeys, selectedRows) => {
        this.setState({
          selectedRows: selectedRows.filter(record => !record.disable)
        })
      },
      getCheckboxProps: record => ({
        disabled: record.disable,
        defaultChecked: record.disable
      }),
    }

    const formItemLayout = {
      labelCol: {
        span: 7
      },
      wrapperCol: {
        span: 17
      },
      style: {
        marginBottom: 0
      }
    }
    const {oggModalKey, oggModalVisible, oggModalContent} = this.state
    return (
      <Modal
        className="top-modal"
        visible={visible}
        maskClosable={false}
        key={key}
        onCancel={onClose}
        onOk={this.handleSubmit}
        width={1000}
        title={<div>
          <FormattedMessage
            id="app.components.resourceManage.dataSchema.addTable"
            defaultMessage="添加表"
          />
          {record.ds_type === 'oracle' && (
            <Button style={{marginLeft: 10}} onClick={this.viewOgg}>
              <FormattedMessage id="app.components.resourceManage.dataSource.viewOggScript" defaultMessage="查看OGG脚本" />
            </Button>
          )}
        </div>
        }
      >
        <Form>
          <Row>
            <Col span={8}>
              <FormItem  label={<FormattedMessage
                id="app.components.resourceManage.dataSourceName"
                defaultMessage="数据源名称"
              />} {...formItemLayout}>
                <Input
                  readOnly
                  size="small"
                  type="text"
                  value={`${record.ds_type}/${record.ds_name}`}
                />
              </FormItem>
            </Col>
            <Col span={8}>
              <FormItem label={<FormattedMessage
                id="app.components.resourceManage.dataSchemaName"
                defaultMessage="Schema名称"
              />} {...formItemLayout}>
                <Input
                  readOnly
                  size="small"
                  type="text"
                  value={record.schema_name}
                />
              </FormItem>
            </Col>
            <Col span={8}>
              <FormItem label={<FormattedMessage
                id="app.common.description"
                defaultMessage="描述"
              />} {...formItemLayout}>
                <Input
                  readOnly
                  size="small"
                  type="text"
                  value={record.description}
                />
              </FormItem>
            </Col>
          </Row>
          <Row style={{marginTop: -5}}>
            <Col span={8}>
              <FormItem label={<FormattedMessage
                id="app.common.status"
                defaultMessage="状态"
              />} {...formItemLayout}>
                <Input
                  readOnly
                  size="small"
                  type="text"
                  value={record.status}
                />
              </FormItem>
            </Col>
            <Col span={8}>
              <FormItem label={<FormattedMessage
                id="app.components.resourceManage.sourceTopic"
                defaultMessage="源Topic"
              />} {...formItemLayout}>
                <Input
                  readOnly
                  size="small"
                  type="text"
                  value={record.src_topic}
                />
              </FormItem>
            </Col>
            <Col span={8}>
              <FormItem label={<FormattedMessage
                id="app.components.resourceManage.targetTopic"
                defaultMessage="目标Topic"
              />} {...formItemLayout}>
                <Input
                  readOnly
                  size="small"
                  type="text"
                  value={record.target_topic}
                />
              </FormItem>
            </Col>
          </Row>
        </Form>
        <Spin spinning={loading} tip="正在加载数据中...">
          {!loading ? (
            <Table
              rowKey={record => `${record.tableName}`}
              rowSelection={rowSelection}
              columns={columns}
              dataSource={tableList}
              pagination={false}
              scroll={{x:800, y: 350}}
            />
          ) : (
            <div style={{width: '100%', height: 100}}/>
          )}
        </Spin>
        <AddTableOggModal
          visible={oggModalVisible}
          key={oggModalKey}
          content={oggModalContent}
          onClose={this.closeOgg}
        />
      </Modal>
    )
  }
}

DataSchemaManageAddModal.propTypes = {}
