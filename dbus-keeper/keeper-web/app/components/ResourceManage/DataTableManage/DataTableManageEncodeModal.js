/**
 * @author 戎晓伟
 * @description  Sink信息设置
 */

import React, { PropTypes, Component } from 'react'
import { Spin, Switch,Form, Select, Input, Modal, Table, Icon} from 'antd'
import { FormattedMessage } from 'react-intl'
// 导入样式
import styles from './res/styles/index.less'
import Request from "@/app/utils/request"
import {DATA_TABLE_SAVE_ENCODE_CONFIG} from "@/app/containers/ResourceManage/api"

const FormItem = Form.Item
const Option = Select.Option

@Form.create()
export default class DataTableManageEncodeModal extends Component {

  constructor (props) {
    super(props)
    this.tableWidth = [
      '10%',
      '10%',
      '25%',
      '25%',
      '25%',
      '5%',
    ]
    this.state = {
      dataSource: null,
    }
  }

  componentWillReceiveProps = nextProps => {
    const {encodeConfigList, tableColumnList} = nextProps
    const _encodeConfigList = Object.values(encodeConfigList.result)
    const _tableColumnList = Object.values(tableColumnList.result)

    // 生成列表数据
    const dataSource = _tableColumnList.map(column => {
      _encodeConfigList.forEach(cfg => {
        cfg.fieldName === column.COLUMN_NAME && (column = {
          ...column,
          pluginId: cfg.pluginId,
          encodeType: cfg.encodeType,
          encodeParam: cfg.encodeParam,
          truncate: cfg.truncate,
        })
      })
      return column
    })
    this.setState({
      dataSource: dataSource
    })
  }

  handleEncodeJarChange = (value , record) => {
    const {dataSource} = this.state
    this.setState({
      dataSource: dataSource.map(ds => {
        if (ds.COLUMN_NAME === record.COLUMN_NAME) {
          ds.pluginId = value
          if (value) {
            const encodeTypeList = Object.values(this.props.encodeTypeList.result)
            const encodeType = encodeTypeList.filter(type => type.id === value)[0]
            ds.encodeType = encodeType.encoders.split(',')[0]
          } else {
            ds.encodeType = null
          }
        }
        return ds
      })
    })
  }

  handleEncodeChange = (value, record) => {
    const {dataSource} = this.state
    this.setState({
      dataSource: dataSource.map(ds => {
        ds.COLUMN_NAME === record.COLUMN_NAME && (ds.encodeType = value)
        return ds
      })
    })
  }

  handleParamChange = (value, record) => {
    const {dataSource} = this.state
    this.setState({
      dataSource: dataSource.map(ds => {
        ds.COLUMN_NAME === record.COLUMN_NAME && (ds.encodeParam = value)
        return ds
      })
    })
  }

  handleTruncateChange = (value, record) => {
    const {dataSource} = this.state
    this.setState({
      dataSource: dataSource.map(ds => {
        ds.COLUMN_NAME === record.COLUMN_NAME && (ds.truncate = (value ? 1 : 0))
        return ds
      })
    })
  }

  renderComponent = render => (text, record, index) =>
    render(text, record, index)

  /**
   * @description 默认的render
   */
  renderNomal = (text, record, index) => (
    <div title={text} className={styles.ellipsis}>
      {text}
    </div>
  )

  renderColumnName = (text, record, index) => (
    <div title={text} className={styles.ellipsis}>
      {record.IS_PRIMARY === 'YES' && <Icon style={{color: '#f00'}} type="key" />}{text}
    </div>
  )


  renderEncodeJar = (text, record, index) => {
    let {encodeTypeList} = this.props
    encodeTypeList = Object.values(encodeTypeList.result)
    encodeTypeList = [{id: null, name: <FormattedMessage
        id="app.components.resourceManage.dataTable.notEncode"
        defaultMessage="不脱敏"
      />}, ...encodeTypeList]
    return (
      <div title={text} className={styles.ellipsis}>
        <Select
          showSearch
          optionFilterProp='children'
          className={styles.select}
          placeholder="Select Encode Jar"
          value={text ? text : null}
          onChange={value => this.handleEncodeJarChange(value, record)}
        >
          {encodeTypeList.map(item => (
            <Option
              value={item.id}
              key={item.id}
            >
              {item.name}
            </Option>
          ))}
        </Select>
      </div>
    )
  }

  renderEncodeType = (text, record, index) => {
    let {encodeTypeList} = this.props
    encodeTypeList = Object.values(encodeTypeList.result)
    const encodeType = encodeTypeList.filter(type => type.id === record.pluginId)[0]
    let optionList = []
    encodeType && (optionList = encodeType.encoders.split(','))
    return (
      <div title={text} className={styles.ellipsis}>
        <Select
          disabled={!record.pluginId}
          showSearch
          optionFilterProp='children'
          className={styles.select}
          placeholder="Select Encode Type"
          value={text ? text : null}
          onChange={value => this.handleEncodeChange(value, record)}
        >
          {optionList.map(item => (
            <Option
              value={item}
              key={item}
            >
              {item}
            </Option>
          ))}
        </Select>
      </div>
    )
  }

  renderEncodeParam = (text, record, index) => (
    <div title={text} className={styles.ellipsis}>
      <Input disabled={!record.pluginId} onChange={e => this.handleParamChange(e.target.value, record)} value={text} type="text" />
    </div>
  )

  renderTruncate = (text, record, index) => (
    <div title={text} className={styles.ellipsis}>
      <Switch
        disabled={!record.pluginId}
        onChange={value => this.handleTruncateChange(value, record)}
        size="small"
        checked={text === 1}
      />
    </div>
  )

  handleOk = () => {
    const {encodeConfigList} = this.props
    const desensitizationInformations = Object.values(encodeConfigList.result)
    const {dataSource} = this.state

    const {tableInfo} = this.props
    const tableId = tableInfo.id

    const names = dataSource.map(ds => (ds.COLUMN_NAME))
    let param = {}
    let operationCount = 0
    for (let i = 0; i < names.length; i++) {
      let row = dataSource[i]
      let foundInDesensitizationInformations = false
      for (let j = 0; j < desensitizationInformations.length; j++) {
        //分四类讨论
        if (names[i] === desensitizationInformations[j].fieldName) {
          foundInDesensitizationInformations = true
          //数据库有脱敏信息,弹框中有脱敏信息,执行更新操作
          if (row.pluginId) {
            const rowParam = {
              sql_type: 'update',
              id: desensitizationInformations[j].id,
              table_id: tableId,
              field_name: names[i],
              plugin_id: row.pluginId,
              encode_type: row.encodeType,
              encode_param: row.encodeParam || '',
              truncate: row.truncate || 0,
              update_time: (new Date()).valueOf()
            }
            operationCount++
            param["ruleOperation" + operationCount] = rowParam
            break
          }
          //数据库有脱敏信息,弹框中无脱敏信息,执行删除操作
          else {
            const rowParam = {
              sql_type: 'delete',
              id: desensitizationInformations[j].id
            }
            operationCount++
            param["ruleOperation" + operationCount] = rowParam
            break
          }
        }
      }
      //数据库无脱敏信息,弹框中有脱敏信息,执行添加操作
      if (!foundInDesensitizationInformations && row.pluginId) {
        let rowParam = {
          sql_type: 'insert',
          table_id: tableId,
          field_name: names[i],
          plugin_id: row.pluginId,
          encode_type: row.encodeType,
          encode_param: row.encodeParam || '',
          truncate: row.truncate || 0,
          update_time: (new Date()).valueOf()
        }
        operationCount++
        param["ruleOperation" + operationCount] = rowParam
      }
      //数据库无脱敏信息,弹框中无脱敏信息,无任何操作
    }
    const {onRequest, onClose, saveApi} = this.props
    onRequest({
      api: saveApi,
      data: param,
      method: 'post',
      callback: onClose
    })
  }

  render () {
    const { visible, key} = this.props
    const { onClose } = this.props
    const {encodeConfigList, tableColumnList, encodeTypeList} = this.props
    const {dataSource} = this.state
    const columns = [
      {
        title: <FormattedMessage
          id="app.components.projectManage.encodeManager.field_name"
          defaultMessage="列名"
        />,
        width: this.tableWidth[0],
        dataIndex: 'COLUMN_NAME',
        key: 'COLUMN_NAME',
        render: this.renderComponent(this.renderColumnName)
      },
      {
        title: <FormattedMessage
          id="app.common.type"
          defaultMessage="类型"
        />,
        width: this.tableWidth[1],
        dataIndex: 'DATA_TYPE',
        key: 'DATA_TYPE',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: <FormattedMessage
          id="app.components.projectManage.encodeManager.encode_plugin_id"
          defaultMessage="脱敏插件ID"
        />,
        width: this.tableWidth[2],
        dataIndex: 'pluginId',
        key: 'pluginId',
        render: this.renderComponent(this.renderEncodeJar)
      },
      {
        title: <FormattedMessage
          id="app.components.projectManage.encodeManager.encode_type"
          defaultMessage="脱敏类型"
        />,
        width: this.tableWidth[3],
        dataIndex: 'encodeType',
        key: 'encodeType',
        render: this.renderComponent(this.renderEncodeType)
      },
      {
        title: <FormattedMessage
          id="app.components.projectManage.encodeManager.encode_param"
          defaultMessage="脱敏参数"
        />,
        width: this.tableWidth[4],
        dataIndex: 'encodeParam',
        key: 'encodeParam',
        render: this.renderComponent(this.renderEncodeParam)
      },
      {
        title: <FormattedMessage
          id="app.components.projectManage.projectHome.tabs.resource.truncate"
          defaultMessage="截取"
        />,
        width: this.tableWidth[5],
        dataIndex: 'truncate',
        key: 'truncate',
        render: this.renderComponent(this.renderTruncate)
      }
    ]

    return (
      <Modal
        className="top-modal"
        key={key}
        title={<FormattedMessage
          id="app.components.projectManage.projectHome.tabs.resource.encodesConfig"
          defaultMessage="脱敏配置"
        />}
        width={1000}
        visible = {visible}
        onCancel={onClose}
        onOk={this.handleOk}
        maskClosable={false}
      >
        <Form>
          <Spin spinning={encodeConfigList.loading || tableColumnList.loading || encodeTypeList.loading} tip="正在加载数据中...">
            {!encodeConfigList.loading && ! tableColumnList.loading  && !encodeTypeList.loading? (
              <Table
                size="small"
                rowKey="COLUMN_NAME"
                dataSource={dataSource}
                columns={columns}
                pagination={false}
                scroll={{x:800, y: 400}}
              />
            ) : (
              <div style={{ height: '378px' }} />
            )}
          </Spin>

        </Form>
      </Modal>
    )
  }
}

DataTableManageEncodeModal.propTypes = {
}
