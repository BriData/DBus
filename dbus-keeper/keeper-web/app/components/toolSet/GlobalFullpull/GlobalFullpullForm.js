/**
 * @author xiancangao
 * @description  基本信息设置
 */
import dateFormat from 'dateformat'
import React, {PropTypes, Component} from 'react'
import { FormattedMessage } from 'react-intl'
import {Checkbox, Tabs, Form, Input, Button, Select} from 'antd'
import JSONTree from 'react-json-tree'
import {TOPO_JAR_START_API} from "@/app/containers/ResourceManage/api";
// import dateFormat from 'dateformat'
import GlobalFullpullStartTopo from './GlobalFullpullStartTopo'
const TabPane = Tabs.TabPane;
const Option = Select.Option
const FormItem = Form.Item
@Form.create({warppedComponentRef: true})
export default class GlobalFullpullForm extends Component {
  constructor(props) {
    super(props)
    this.state = {
      physicalTableRegex: null,
      json: {}
    }
  }

  handleSend = () => {
    this.props.form.validateFields((err, values) => {
      if (!err) {
        const {onGlobalFullPull} = this.props
        const {json} = this.state
        const values = this.props.form.getFieldsValue()
        const table = this.props.dataTableList.filter(table => table.tableName === values.tableName)[0]
        onGlobalFullPull({
          id: json.id,
          type: 'global',
          dsName: table.dsName,
          schemaName: values.schemaName,
          tableName: values.tableName,
          ctrlTopic: values.ctrlTopic,
          outputTopic: values.resultTopic,
          tableOutputTopic: table.resultTopic,
          message: JSON.stringify(json)
        })
      }
    })
  }

  generateJson = (values) => {
    const date = new Date()
    const {physicalTableRegex} = this.state
    this.setState({
      json: {
        "from": "global-pull-request-from-CtrlMsgSender",
        "id": date.getTime(),
        "payload": {
          "DBUS_DATASOURCE_ID": `${values.dsId}`,
          "INCREASE_VERSION": values.INCREASE_VERSION,
          "INCREASE_BATCH_NO": values.INCREASE_BATCH_NO,
          "OP_TS": null,
          "PHYSICAL_TABLES": physicalTableRegex,
          "POS": null,
          "PULL_REMARK": "",
          "PULL_TARGET_COLS": "",
          "SCHEMA_NAME": values.schemaName,
          "SCN_NO": "",
          "SEQNO": date.getTime(),
          "SPLIT_BOUNDING_QUERY": "",
          "SPLIT_COL": "",
          "SPLIT_SHARD_SIZE": "",
          "SPLIT_SHARD_STYLE": "",
          "TABLE_NAME": values.tableName,
          "resultTopic": values.resultTopic
        },
        "timestamp": dateFormat(date, 'yyyy-mm-dd HH:MM:ss.l'),
        "type": "FULL_DATA_INDEPENDENT_PULL_REQ"
      }
    })
  }

  handleIncreaseVersionOrBatchChange = (checked, key) => {
    const {setFieldsValue, getFieldsValue} = this.props.form
    setFieldsValue({[key]: checked})
    this.generateJson({
      ...getFieldsValue()
    })
  }

  handleDsIdChange = value => {
    const {onSearchSchema} = this.props
    onSearchSchema(value)
    this.props.form.setFieldsValue({schemaName: null, tableName: null})
  }

  handleSchemaNameChange = schemaName => {
    const {onSearchTable} = this.props
    const values = this.props.form.getFieldsValue()
    onSearchTable(values.dsId, schemaName)
    this.props.form.setFieldsValue({tableName: null})
  }

  handleTableNameChange = tableName => {
    const {getFieldsValue, setFieldsValue} = this.props.form
    const table = this.props.dataTableList.filter(table => table.tableName === tableName)[0]
    setFieldsValue({
      tableName,
      resultTopic: `global.${table.dsName}.${table.schemaName}.${table.tableName}.result`
    })
    this.setState({
      physicalTableRegex: table.physicalTableRegex
    }, () => this.generateJson({
      ...getFieldsValue(),
    }))
  }

  handleResultTopicChange = resultTopic => {
    const {setFieldsValue, getFieldsValue} = this.props.form
    setFieldsValue({resultTopic})
    this.generateJson({
      ...getFieldsValue(),
    })
  }

  render() {
    const {getFieldDecorator} = this.props.form
    const {dataSourceList, dataSchemaList, dataTableList, jarInfos} = this.props
    const formItemLayout = {
      labelCol: {span: 4},
      wrapperCol: {span: 10}
    }

    const {json} = this.state

    const theme = {
      scheme: 'monokai',
      base00: '#272822'
    }
    return (
      <div className="form-search">
        <Tabs
          defaultActiveKey="global"
        >
          <TabPane
            tab={<FormattedMessage
              id="app.components.toolset.globalFullPull.globalFullPull"
              defaultMessage="全局独立拉全量"
            />}
            key="global"
          >

            <Form autoComplete="off" layout="horizontal">
              <FormItem
                label={<FormattedMessage
                  id="app.components.toolset.globalFullPull.increaseVersion"
                  defaultMessage="增加版本"
                />}
                {...formItemLayout}
              >
                {getFieldDecorator('INCREASE_VERSION', {
                  initialValue: false
                })(
                  <Checkbox
                    onChange={({target: {checked}}) => this.handleIncreaseVersionOrBatchChange(checked, 'INCREASE_VERSION')}/>
                )}
              </FormItem>
              <FormItem
                label={<FormattedMessage
                  id="app.components.toolset.globalFullPull.increaseBatchNo"
                  defaultMessage="增加批次号"
                />}
                {...formItemLayout}
              >
                {getFieldDecorator('INCREASE_BATCH_NO', {
                  initialValue: false
                })(
                  <Checkbox
                    onChange={({target: {checked}}) => this.handleIncreaseVersionOrBatchChange(checked, 'INCREASE_BATCH_NO')}/>
                )}
              </FormItem>
              <FormItem
                label={<FormattedMessage
                  id="app.components.toolset.controlMessage.dataSource"
                  defaultMessage="数据源"
                />}
                {...formItemLayout}
              >
                {getFieldDecorator('dsId', {
                  rules: [
                    {
                      required: true,
                      message: '请选择数据源'
                    }
                  ]
                })(
                  <Select
                    showSearch
                    optionFilterProp="children"
                    onChange={this.handleDsIdChange}
                    placeholder="select a data source"
                    filterOption={(input, option) => option.props.children.toLowerCase().indexOf(input.toLowerCase()) >= 0}
                  >
                    {dataSourceList.map(item => (
                      <Option value={item.dsId ? item.dsId : null} key={`${item.dsId ? item.dsId : 'dsId'}`}>
                        {item.dsTypeName}
                      </Option>
                    ))}
                  </Select>
                )}
              </FormItem>
              <FormItem
                label={<FormattedMessage
                  id="app.components.resourceManage.dataSchemaName"
                  defaultMessage="Schema名称"
                />}
                {...formItemLayout}
              >
                {getFieldDecorator('schemaName', {
                  rules: [
                    {
                      required: true,
                      message: '请选择Schema'
                    }
                  ]
                })(
                  <Select
                    showSearch
                    optionFilterProp="children"
                    onChange={this.handleSchemaNameChange}
                    placeholder="select a data schema"
                    filterOption={(input, option) => option.props.children.toLowerCase().indexOf(input.toLowerCase()) >= 0}
                  >
                    {dataSchemaList.map(item => (
                      <Option value={item.schemaName ? item.schemaName : null}
                              key={`${item.schemaName ? item.schemaName : 'schemaName'}`}>
                        {item.schemaName}
                      </Option>
                    ))}
                  </Select>
                )}
              </FormItem>
              <FormItem
                label={<FormattedMessage
                  id="app.components.resourceManage.dataTableName"
                  defaultMessage="表名"
                />}
                {...formItemLayout}
              >
                {getFieldDecorator('tableName', {
                  rules: [
                    {
                      required: true,
                      message: '请选择Table'
                    }
                  ]
                })(
                  <Select
                    showSearch
                    optionFilterProp="children"
                    onChange={this.handleTableNameChange}
                    placeholder="select a data table"
                    filterOption={(input, option) => option.props.children.toLowerCase().indexOf(input.toLowerCase()) >= 0}
                  >
                    {dataTableList.map(item => (
                      <Option value={item.tableName ? item.tableName : null}
                              key={`${item.tableName ? item.tableName : 'tableName'}`}>
                        {item.tableName}
                      </Option>
                    ))}
                  </Select>
                )}
              </FormItem>
              <FormItem
                label="Ctrl Topic"
                {...formItemLayout}
              >
                {getFieldDecorator('ctrlTopic', {
                  initialValue: 'global_ctrl_topic',
                  rules: [
                    {
                      required: true,
                      message: '请输入ctrlTopic'
                    }
                  ]
                })(
                  <Input disabled
                  />
                )}
              </FormItem>
              <FormItem
                label="Result Topic"
                {...formItemLayout}
              >
                {getFieldDecorator('resultTopic', {
                  rules: [
                    {
                      required: true,
                      message: '请输入resultTopic'
                    }
                  ]
                })(
                  <Input
                    onChange={({target: {value}}) => this.handleResultTopicChange(value)}
                  />
                )}
              </FormItem>

              <FormItem
                wrapperCol={{
                  xs: {span: 4, offset: 0},
                  sm: {span: 10, offset: 4}
                }}
              >
                <JSONTree data={json} theme={theme}/>
              </FormItem>
              <FormItem
                wrapperCol={{
                  xs: {span: 24, offset: 0},
                  sm: {span: 16, offset: 4}
                }}
              >
                <Button onClick={this.handleSend} type="primary" htmlType="submit">
                  <FormattedMessage
                    id="app.components.toolset.globalFullPull.sendFullPullRequest"
                    defaultMessage="发送独立拉全量请求"
                  />
                </Button>
              </FormItem>
            </Form>
          </TabPane>
          <TabPane
            tab={<FormattedMessage
              id="app.components.toolset.globalFullPull.startTopology"
              defaultMessage="启动拓扑"
            />}
            key="start"
          >
            <GlobalFullpullStartTopo
              jarInfos={jarInfos}
              topoJarStartApi={TOPO_JAR_START_API}
            />
          </TabPane>
        </Tabs>
      </div>
    )
  }
}

GlobalFullpullForm.propTypes = {
  form: PropTypes.object,
  messageTypeChanged: PropTypes.func
}
