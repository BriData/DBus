import React, {PropTypes, Component} from 'react'
import {Button, Row, Col, Modal, Form, Select, Input, Spin, Table, Icon, message} from 'antd'
import {FormattedMessage} from 'react-intl'
import OperatingButton from '@/app/components/common/OperatingButton'
import AddSchemaOggModal from './AddSchemaOggModal'
// 导入样式
import styles from './res/styles/index.less'
import Request from "@/app/utils/request";

const FormItem = Form.Item
const Option = Select.Option
const TextArea = Input.TextArea

@Form.create()
export default class DataSourceManageAddModal extends Component {
  constructor(props) {
    super(props)
    this.state = {
      recordRowKeys: {},
      schemaInfos: {},
      tableInfos: {},
      currentSchema: null,

      oggModalKey: 'oggModalKey',
      oggModalVisible: false,
      oggModalContent: null
    }
    this.tableWidth = [
      '20%',
      '20%',
      '20%',
      '20%',
      '20%',
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

  handleSchemaChange = value => {
    const {getSchemaTableList, record} = this.props
    this.setState({currentSchema: value},() => getSchemaTableList({
      dsId: record.id,
      dsName: record.name,
      schemaName: value
    }))
  }

  handleSubmit = () => {
    const {onClose, addApi, record} = this.props
    const {schemaInfos, tableInfos} = this.state
    const data = {
      dataSource: {
        id: record.id,
        dsName:record.name,
        dsType:record.type
      }
    }
    if(Object.keys(schemaInfos).length === 0) {
      message.warn("未选中任何表")
      return
    }
    data.schemaAndTables = Object.keys(schemaInfos).map(schemaName => {
      const schema = schemaInfos[schemaName]
      const tables = tableInfos[schemaName]
      return {
        schema: {
          dsId: schema.dsId,
          /**
           * 这样写是因为SchemaTable中没有schema信息，只能从schemaList中获取schema信息
           * 而schemaList中没有dsName和dsType这两个属性，因此使用record的属性
           */
          dsName: schema.dsName || record.name,
          dsType: schema.dsType || record.type,
          schemaName: schema.schemaName,
          srcTopic: schema.srcTopic,
          targetTopic: schema.targetTopic,
        },
        tables: tables.map(table => ({
          schemaName: schema.schemaName,
          tableName: table.tableName,
          physicalTableRegex: table.physicalTableRegex,
          outputTopic: table.outputTopic,
        }))
      }
    })

    Request(addApi, {
      data: data,
      method: 'post' })
      .then(res => {
        if (res && res.status === 0) {
          message.success("添加schema完成")
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

  beforeViewOgg = () => {
    const {schemaList, schemaTableResult} = this.props
    const schemaTable = schemaTableResult.result.payload
    const {recordRowKeys, currentSchema} = this.state
    // 在select处需要根据这里有无schema信息来显示标识
    const {schemaInfos} = this.state
    let schemaInfo = schemaTable && schemaTable.schema
    /**
     * 如果返回的SchemaTable中没有Schema信息，则从schemaList中获取schema的信息来显示
     * 这种情况表明管理库中没有该schema
     */
    if (!schemaInfo) {
      if (currentSchema) {
        schemaInfo = schemaList.filter(schema => schema.schemaName === currentSchema)[0]
      } else {
        schemaInfo = {}
      }
    }
    let tableList = schemaTable && schemaTable.tables || []
    tableList = tableList.map(table => ({
      ...table,
      outputTopic: table.outputTopic || schemaInfo.targetTopic,
      physicalTableRegex: table.physicalTableRegex || table.tableName
    }))

    const selectedRows = tableList.filter(table => table.disable)
    const selectedRowKeys = selectedRows.map(table => table.tableName)

    if (!currentSchema) {
      this.viewOgg()
      return
    }
    if (recordRowKeys[currentSchema]) {
      this.viewOgg()
      return
    }
    recordRowKeys[currentSchema] = [...new Set(selectedRowKeys)]

    schemaInfos[currentSchema] = schemaInfo

    const filteredSelectedRows = selectedRows.filter(record => !record.disable)
    const {tableInfos} = this.state
    tableInfos[currentSchema] = filteredSelectedRows

    this.setState({recordRowKeys, schemaInfos, tableInfos}, () => this.viewOgg())
  }


  viewOgg = () => {
    const {recordRowKeys, schemaInfos, tableInfos} = this.state
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
    const {visible, key, record, onClose, schemaList, schemaTableResult} = this.props
    const loading = schemaTableResult.loading
    const schemaTable = schemaTableResult.result.payload
    const {recordRowKeys, currentSchema} = this.state
    const selectedRowKeys = currentSchema && recordRowKeys[currentSchema] ? recordRowKeys[currentSchema] : []
    // 在select处需要根据这里有无schema信息来显示标识
    const {schemaInfos} = this.state
    let schemaInfo = schemaTable && schemaTable.schema
    /**
     * 如果返回的SchemaTable中没有Schema信息，则从schemaList中获取schema的信息来显示
     * 这种情况表明管理库中没有该schema
     */
    if (!schemaInfo) {
      if (currentSchema) {
        schemaInfo = schemaList.filter(schema => schema.schemaName === currentSchema)[0]
      } else {
        schemaInfo = {}
      }
    }
    let tableList = schemaTable && schemaTable.tables || []
    tableList = tableList.map(table => ({
      ...table,
      outputTopic: table.outputTopic || schemaInfo.targetTopic,
      physicalTableRegex: table.physicalTableRegex || table.tableName
    }))
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
      selectedRowKeys,
      onChange: (selectedRowKeys, selectedRows) => {
        // 此处会把defaultChecked的key值push进来，所以需要使用Set去重
        const {currentSchema,recordRowKeys} = this.state
        recordRowKeys[currentSchema] = [...new Set(selectedRowKeys)]

        const {schemaInfos} = this.state
        schemaInfos[currentSchema] = schemaInfo

        const filteredSelectedRows = selectedRows.filter(record => !record.disable)
        const {tableInfos} = this.state
        tableInfos[currentSchema] = filteredSelectedRows

        this.setState({recordRowKeys, schemaInfos, tableInfos})
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
        title={
          <div>
          <span><FormattedMessage id="app.common.addSchema" defaultMessage="添加Schema" /></span>
          {record.type === 'oracle' && (
          <Button style={{marginLeft: 10}} onClick={this.beforeViewOgg}>
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
                  value={`${record.type}/${record.name}`}
                />
              </FormItem>
            </Col>
            <Col span={8}>
              <FormItem label={<FormattedMessage
                id="app.components.resourceManage.dataSchemaName"
                defaultMessage="Schema名称"
              />} {...formItemLayout}>
                <div style={{marginTop: 5}}>
                  <Select
                    showSearch
                    optionFilterProp='children'
                    style={{ width: '100%'}}
                    size="small"
                    value={currentSchema}
                    onChange={this.handleSchemaChange}
                  >
                    {schemaList &&
                    schemaList.map(item => (
                      <Option value={item.schemaName} key={item.schemaName}>
                        {item.schemaName+" "}
                        {schemaInfos[item.schemaName] && (<Icon style={{color: '#00C1DE'}} type="info-circle-o" />)}
                      </Option>
                    ))}
                  </Select>
                </div>
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
                  value={schemaInfo.description}
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
                  value={schemaInfo.status}
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
                  value={schemaInfo.srcTopic}
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
                  value={schemaInfo.targetTopic}
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
        <AddSchemaOggModal
          visible={oggModalVisible}
          key={oggModalKey}
          content={oggModalContent}
          onClose={this.closeOgg}
        />
      </Modal>
    )
  }
}

DataSourceManageAddModal.propTypes = {}
