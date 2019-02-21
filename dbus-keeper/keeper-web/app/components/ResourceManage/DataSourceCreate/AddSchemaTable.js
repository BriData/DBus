import React, {PropTypes, Component} from 'react'
import {Form, Select, Input, Row, Col, Button, Tabs, Spin, Table, message , Icon} from 'antd'
const TabPane = Tabs.TabPane;
const Textarea = Input.TextArea
// 导入样式
import styles from './res/styles/index.less'
import Request from "@/app/utils/request";

const FormItem = Form.Item
const Option = Select.Option

@Form.create()
export default class AddSchemaTable extends Component {
  constructor(props) {
    super(props)
    this.state = {
      recordRowKeys: {},
      schemaInfos: {},
      tableInfos: {},
      currentSchema: null,


      nextLoading: false
    }
    this.tableWidth = [
      '20%',
      '20%',
      '20%',
      '20%',
      '20%',
    ]
  }

  componentWillMount = () => {
    const {getSchemaListByDsId, dataSource} = this.props
    getSchemaListByDsId({dsId: dataSource.id})
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
    const {getSchemaTableList, dataSource} = this.props
    this.setState({currentSchema: value},() => getSchemaTableList({
      dsId: dataSource.id,
      dsName: dataSource.dsName,
      schemaName: value
    }))
  }

  handleNext = () => {
    const {addSchemaTableApi, dataSource} = this.props
    const {schemaInfos, tableInfos} = this.state
    const data = {
      dataSource: {
        id: dataSource.id,
        dsName:dataSource.dsName,
        dsType:dataSource.dsType
      }
    }
    // if(Object.keys(schemaInfos).length === 0) {
    //   message.warn("未选中任何表")
    //   return
    // }
    data.schemaAndTables = Object.keys(schemaInfos).map(schemaName => {
      const schema = schemaInfos[schemaName]
      const tables = tableInfos[schemaName]
      return {
        schema: {
          dsId: schema.dsId,
          /**
           * 这样写是因为SchemaTable中没有schema信息，只能从schemaList中获取schema信息
           * 而schemaList中没有dsName和dsType这两个属性，因此使用dataSource的属性
           */
          dsName: schema.dsName || dataSource.dsName,
          dsType: schema.dsType || dataSource.dsType,
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

    this.setState({nextLoading: true})
    Request(addSchemaTableApi, {
      data: data,
      method: 'post' })
      .then(res => {
        if (res && res.status === 0) {
          this.handleAutoClone()
        } else {
          this.setState({nextLoading: false})
          message.warn(res.message)
        }
      })
      .catch(error => {
        this.setState({nextLoading: false})
        message.error(error)
      })
  }

  handleAutoClone = () => {
    const {cloneConfFromTemplateApi, dataSource} = this.props
    Request(cloneConfFromTemplateApi, {
      params: {
        dsName: dataSource.dsName,
        dsType: dataSource.dsType
      },
      method: 'get' })
      .then(res => {
        this.setState({nextLoading: false})
        if (res && res.status === 0) {
          message.success('自动克隆ZK完成')
          const {onAutoCloneZkSuccess} = this.props
          onAutoCloneZkSuccess()
        } else {
          message.warn(res.message)
          const {onAutoCloneZkFail} = this.props
          onAutoCloneZkFail()
        }
      })
      .catch(error => {
        this.setState({nextLoading: false})
        const {onAutoCloneZkFail} = this.props
        onAutoCloneZkFail()
        error.response.data && error.response.data.message
          ? message.error(error.response.data.message)
          : message.error(error.message)
      })
  }

  handleRandom = key =>
    `${Math.random()
      .toString(32)
      .substr(3, 8)}${key || ''}`


  render() {
    const {dataSource} = this.props
    const {schemaList, schemaTableResult} = this.props
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
    const columns = [
      {
        title: (
          'tableName'
        ),
        width: this.tableWidth[0],
        dataIndex: 'tableName',
        key: 'tableName',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          'physicalTableRegex'
        ),
        width: this.tableWidth[1],
        dataIndex: 'physicalTableRegex',
        key: 'physicalTableRegex',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          'outputTopic'
        ),
        width: this.tableWidth[2],
        dataIndex: 'outputTopic',
        key: 'outputTopic',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          'incompatibleColumn'
        ),
        width: this.tableWidth[2],
        dataIndex: 'incompatibleColumn',
        key: 'incompatibleColumn',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          'ignoreColumn'
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

        // 如果选中的所有内容都是已经存在的，则删除schemaInfos和tableInfos相关信息
        if (filteredSelectedRows.length === 0) {
          delete schemaInfos[currentSchema]
          delete tableInfos[currentSchema]
        }
        if (selectedRows.length === 0) {
          delete recordRowKeys[currentSchema]
        }

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
    const tailFormItemLayout = {
      wrapperCol: {
        span: 6,
        offset: 18,
      }
    }


    const {nextLoading} = this.state
    return (
      <div>
        <Form>
          <Row>
            <Col span={8}>
              <FormItem  label={'dataSource'} {...formItemLayout}>
                <Input
                  readOnly
                  size="small"
                  type="text"
                  value={`${dataSource.dsType}/${dataSource.dsName}`}
                />
              </FormItem>
            </Col>
            <Col span={8}>
              <FormItem label={'Schema'} {...formItemLayout}>
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
              <FormItem label={'Description'} {...formItemLayout}>
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
              <FormItem label={'status'} {...formItemLayout}>
                <Input
                  readOnly
                  size="small"
                  type="text"
                  value={schemaInfo.status}
                />
              </FormItem>
            </Col>
            <Col span={8}>
              <FormItem label={'src_topic'} {...formItemLayout}>
                <Input
                  readOnly
                  size="small"
                  type="text"
                  value={schemaInfo.srcTopic}
                />
              </FormItem>
            </Col>
            <Col span={8}>
              <FormItem label={'target_topic'} {...formItemLayout}>
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
            />
          ) : (
            <div style={{width: '100%', height: 100}}/>
          )}
        </Spin>
        <Form autoComplete="off" style={{marginTop: 10}}>
          <FormItem {...tailFormItemLayout}>
            <Button loading={nextLoading} type="primary" onClick={this.handleNext}>下一步</Button>
          </FormItem>
        </Form>
      </div>
    )
  }
}

AddSchemaTable.propTypes = {}
