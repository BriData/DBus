import React, { PropTypes, Component } from 'react'
import {
  Form,
  Table,
  Button,
  Input,
  Select,
  Radio,
  Switch,
  Icon,
  Spin,
  Tooltip,
  message
} from 'antd'
import { FormattedMessage } from 'react-intl'
import { intlMessage } from '@/app/i18n'
import { fromJS } from 'immutable'
const FormItem = Form.Item
const RadioGroup = Radio.Group
const Option = Select.Option
// 导入样式
import styles from '../../../res/styles/index.less'
import {SEARCH_TABLE_RESOURCES_COLUMNS_LIST_API} from '@/app/containers/ProjectManage/api/index.js'
import Request, {getUserInfo} from "@/app/utils/request";

export default class EncodeConfig extends Component {
  constructor (props) {
    super(props)
    this.NomalTableWidth = ['10%', '10%', '10%','10%','15%', '15%', '15%', '5%', '10%']
    this.state = {
      encodeOutputColumns: {},
      encodeSourceSelected: [],
      outputListType: '0'
    }
  }
  componentWillMount () {
    const {tid, projectId} = this.props
    Request(SEARCH_TABLE_RESOURCES_COLUMNS_LIST_API, {
      params: {
        tableId: tid,
        projectId: projectId
      },
      method: 'get' })
      .then(res => {
        if (res && res.status === 0) {
          // 初始化赋值
          const { encodes } = this.props
          let encodeOutputColumns
          if(encodes && encodes[tid] && encodes[tid].encodeOutputColumns) {
            encodeOutputColumns = fromJS(encodes[tid].encodeOutputColumns)
          } else {
            const { payload } = res
            if (payload && payload.length === 0) {
              message.warn('该表无列信息', 2)
            }
            let temporaryEncodeSource = {}
            Object.values(payload).map(item => {
              const es = item.encodeSource
              temporaryEncodeSource[`_${item.cid}`] = {
                ...item,
                tableId: item.tid,
                fieldName: item.columnName,
                encodeSource: es === 0 || es ? String(es) : '3'
              }
            })
            encodeOutputColumns = fromJS({
              ...temporaryEncodeSource,
            })
          }
          const outputListType =
            (encodes && encodes[tid] && encodes[tid].outputListType.toString()) ||
            '0'
          this.setState({
            encodeOutputColumns: encodeOutputColumns.toJS(),
            outputListType: outputListType
          })
          // 向父层传递数据
          this.handleSaveToReudx(tid, {
            encodeOutputColumns: encodeOutputColumns.toJS(),
            outputListType
          })
        } else {
          message.warn(res.message)
        }
      })
      .catch(error => {
        error.response && error.response.data && error.response.data.message
          ? message.error(error.response.data.message)
          : message.error(error.message)
      })
  }
  /**
   * @description 将选中的encodeID过滤并添加到临时encodeOutputColumns中
   */
  handleAddEncode = () => {
    const { tid } = this.props
    const { encodeSourceSelected, outputListType } = this.state
    const { result } = this.props.encodeList
    let temporaryEncodeSource = {}
    // 根据ID过滤
    encodeSourceSelected.forEach(item => {
      temporaryEncodeSource[`_${item}`] = Object.values(result)
        .filter(data => item === `${data.cid}`)
        .map(item => ({
          ...item,
          tableId: item.tid,
          fieldName: item.columnName
        }))[0]
    })
    this.setState({
      encodeOutputColumns: {
        ...temporaryEncodeSource,
        ...this.state.encodeOutputColumns
      }
    })
    // 向父层传递数据
    this.handleSaveToReudx(tid, {
      encodeOutputColumns: {
        ...temporaryEncodeSource,
        ...this.state.encodeOutputColumns
      },
      outputListType
    })
  };

  /**
   * @param key [object String] 已选encode对应的key值
   * @description 删除临时encodeOutputColumns对应的encode
   */
  handleDelEncode = (e, key) => {
    const { tid } = this.props
    const { encodeOutputColumns, outputListType } = this.state
    // 删除某项encode
    const temporaryEncodeSource = fromJS(encodeOutputColumns).delete(`_${key}`)
    this.setState({ encodeOutputColumns: temporaryEncodeSource.toJS() })
    // 向父层传递数据
    this.handleSaveToReudx(tid, {
      encodeOutputColumns: { ...temporaryEncodeSource.toJS() },
      outputListType
    })
  };

  /**
   * @param value [object Array] 已选encodeID数组
   * @description 将选中的encodeID 存储到临时encodeSourceSelected中
   */
  handleSelectEncodes = value => {
    this.setState({ encodeSourceSelected: value })
  };
  /**
   * @description 根据ID判断是否存在encodeType属性
   */
  handleFilterEncodeType = key => {
    const { result } = this.props.encodeList
    const value = Object.values(result).filter(item => item.cid === key)[0]
    return !!(value && value['disable'])
  };

  /**
   * @param value [object String] 值
   * @param key  [object String]  需要匹配查询的ID
   * @param type [object String]  修改的属性
   * @description 修改encode属性并临时存储
   */
  handleChangeEncode = (value, key, type) => {
    const { tid } = this.props
    const { encodeOutputColumns, outputListType } = this.state
    let temporaryEncodeSource = fromJS(encodeOutputColumns).setIn(
      [`_${key}`, type],
      value
    )
    this.setState({ encodeOutputColumns: temporaryEncodeSource.toJS() })
    // 向父层传递数据
    this.handleSaveToReudx(tid, {
      encodeOutputColumns: { ...temporaryEncodeSource.toJS() },
      outputListType
    })
  };
  /**
   * @param tid table ID
   * @param source 向redux存储的数据
   * @description 合并数据并传递给父层
   */
  handleSaveToReudx = (tid, source) => {
    const { encodes, onChange } = this.props
    const temporaryEncodeSource = {}
    temporaryEncodeSource[tid] = source
    // 向父层传递数据
    onChange({ ...encodes, ...temporaryEncodeSource })
  };
  /**
   * @deprecated input placeholder
   */
  handlePlaceholder = fun => id =>
    fun({
      id: 'app.components.select.placeholder',
      valus: {
        name: fun({ id })
      }
    });
  /**
   * @description 选择输出类型onChange
   */
  handleRadioChange = e => {
    const { tid } = this.props
    const value = e.target.value
    if (value === '0') {
      this.encodeOutputColumns = JSON.parse(
        JSON.stringify(this.state.encodeOutputColumns)
      )
      if (this.encodeSourceAllList) {
        this.setState({ encodeOutputColumns: { ...this.encodeSourceAllList } })
        // 向父层传递数据
        this.handleSaveToReudx(tid, {
          encodeOutputColumns: { ...this.encodeSourceAllList },
          outputListType: value
        })
      } else {
        this.handleSelectAll()
      }
    } else {
      this.encodeSourceAllList = JSON.parse(
        JSON.stringify(this.state.encodeOutputColumns)
      )
      this.setState({ encodeOutputColumns: { ...this.encodeOutputColumns } })
      // 向父层传递数据
      this.handleSaveToReudx(tid, {
        encodeOutputColumns: { ...this.encodeOutputColumns },
        outputListType: value
      })
    }
    this.setState({ outputListType: value })
  };
  /**
   * @description 全选
   */
  handleSelectAll = () => {
    const { tid } = this.props
    const { result } = this.props.encodeList
    let temporaryEncodeSource = {}
    Object.values(result).map(item => {
      temporaryEncodeSource[`_${item.cid}`] = {
        ...item,
        tableId: item.tid,
        fieldName: item.columnName
      }
    })
    this.setState({
      encodeOutputColumns: {
        ...temporaryEncodeSource,
        ...this.state.encodeOutputColumns
      }
    })
    // 向父层传递数据
    this.handleSaveToReudx(tid, {
      encodeOutputColumns: {
        ...temporaryEncodeSource,
        ...this.state.encodeOutputColumns
      },
      outputListType: '0'
    })
  };
  /**
   * @param render 传入一个render
   * @returns render 返回一个新的render
   * @description 统一处理render函数
   */
  renderComponent = render => (text, record, index) =>
    render(text, record, index);

  /**
   * @description encodeConfig默认的render
   */
  renderNomal = width => (text, record, index) => {
    let style = {}
    let hoverText = text
    // 1代表源端删除了该列，此处给出提示，不需要用户自己删除
    if (record.schemaChangeFlag === 1) {
      style = {color : "#FF0000"}
      hoverText = `保存Table信息后将自动删除该列${record.schemaChangeComment}`
    } else if (record.schemaChangeFlag === 2) {
      // 列类型变更，只需要提示用户自己更改
      style = {color : "#0000ff"}
      hoverText = `此列类型已发生变更${record.schemaChangeComment}`
    }
    return (
      <div
        title={`${hoverText}`}

        className={styles.ellipsis}
      >
        {text}
      </div>
    )
  }
  /**
   * @description 脱敏要求
   */
  renderEncodeNeed = width => (text, record, index) => {
    const str = text === 0 || text ? String(text) : '3'
    if (text === undefined) {
      this.handleChangeEncode('3', record.cid, 'encodeSource')
    }
    switch (str) {
      case '0':
        return <span>DBA脱敏</span>
      case '1':
        return <span>项目级脱敏</span>
      default:
        return (
          <Select
            showSearch
            optionFilterProp='children'
            size="small"
            onChange={value =>
              this.handleChangeEncode(value, record.cid, 'encodeSource')
            }
            value={str || '3'}
            style={{ width: '100%' }}
          >
            <Option value={'3'} key="no">
              无
            </Option>
            <Option value={'2'} key="nomal">
              自定义脱敏
            </Option>
          </Select>
        )
    }
  };

  renderSpecialApprove = width => (text, record, index) => {
    const userInfo = getUserInfo()
    return (
      <div
        title={text}

      >
        <Switch
          checked={
            text === 1
          }
          size="small"
          checkedChildren={<Icon type="check"/>}
          unCheckedChildren={<Icon type="cross"/>}
          disabled={userInfo.roleType !== "admin"}
          onChange={value =>
            this.handleChangeEncode(Number(value), record.cid, 'specialApprove')
          }
        />
      </div>
    )
  }

  /**
   * @description encodeConfig的脱敏插件
   */
  renderEncodePlugin = width => (text, record, index) => {
    const { locale, encodeTypeList } = this.props
    const { result, loading } = encodeTypeList
    const plugins = result.plugins || []
    const localeMessage = intlMessage(locale)
    const placeholder = this.handlePlaceholder(localeMessage)
    const encodeSource =
      record['encodeSource'] || record['encodeSource'] === 0
        ? String(record['encodeSource'])
        : '3'
    const showComponent = encodeSource !== '3'
    return (
      <div

        className={styles.ellipsis}
      >
        {showComponent ? (
          <Select
            showSearch
            optionFilterProp='children'
            style={{ width: 150 }}
            notFoundContent={loading ? <Spin size="small" /> : null}
            placeholder={placeholder(
              'app.components.projectManage.projectHome.tabs.resource.encodePlugin'
            )}
            disabled={this.handleFilterEncodeType(record.cid)}
            onChange={value => {
              this.handleChangeEncode(value, record.cid, 'encodePluginId')
              setTimeout(() => this.handleChangeEncode(null, record.cid, 'encodeType'))
            }}
            value={text || null}
            size="small"
          >
            {plugins.map(item => (
              <Option value={item.id} key={item.id}>
                {item.name}
              </Option>
            ))}
          </Select>
        ) : (
          ''
        )}
      </div>
    )
  };

  /**
   * @description encodeConfig的脱敏类型|脱敏规则
   */
  renderEncodeType = width => (text, record, index) => {
    const { locale, encodeTypeList } = this.props
    const { result, loading } = encodeTypeList
    const defaultEncoders = result.defaultEncoders || []
    const plugins = result.plugins || []
    const localeMessage = intlMessage(locale)
    const placeholder = this.handlePlaceholder(localeMessage)
    const encodeSource =
      record['encodeSource'] || record['encodeSource'] === 0
        ? String(record['encodeSource'])
        : '3'
    const showComponent = encodeSource !== '3'

    const renderOptions = () => {
      if (!record.encodePluginId) {
        return defaultEncoders.map(item => (
          <Option value={item} key={item}>
            {item}
          </Option>
        ))
      } else {
        const plugin = plugins.filter(plugin => plugin.id === record.encodePluginId)[0] || {}
        const encoderString = plugin.encoders || ""
        return encoderString.split(",").map(item => (
          <Option value={item} key={item}>
            {item}
          </Option>
        ))
      }
    }

    return (
      <div

        className={styles.ellipsis}
      >
        {showComponent ? (
          <Select
            showSearch
            optionFilterProp='children'
            style={{ width: 150 }}
            notFoundContent={loading ? <Spin size="small" /> : null}
            placeholder={placeholder(
              'app.components.projectManage.projectHome.tabs.resource.encodeType'
            )}
            disabled={this.handleFilterEncodeType(record.cid)}
            onChange={value =>
              this.handleChangeEncode(value, record.cid, 'encodeType')
            }
            value={text || null}
            size="small"
          >
            <Option value={null} style={{ color: '#999' }}>
              {placeholder(
                'app.components.projectManage.projectHome.tabs.resource.encodeType'
              )}
            </Option>
            {renderOptions()}
          </Select>
        ) : (
          ''
        )}
      </div>
    )
  };

  /**
   * @description encodeConfig的脱敏参数
   */
  renderEncodeParam = width => (text, record, index) => {
    const { locale } = this.props
    const encodeSource =
      record['encodeSource'] || record['encodeSource'] === 0
        ? String(record['encodeSource'])
        : '3'
    const showComponent = encodeSource !== '3'
    const localeMessage = intlMessage(locale)
    const handlePlaceholder = fun => id =>
      fun({
        id: 'app.components.input.placeholder',
        valus: {
          name: fun({ id })
        }
      })
    return (
      <div>
        {showComponent ? (
          <Input
            disabled={this.handleFilterEncodeType(record.cid)}
            placeholder={handlePlaceholder(localeMessage)(
              'app.components.projectManage.projectHome.tabs.resource.encodeParam'
            )}
            onChange={e =>
              this.handleChangeEncode(e.target.value, record.cid, 'encodeParam')
            }
            value={text}
            size="small"
          />
        ) : (
          ''
        )}
      </div>
    )
  };

  /**
   * @description encodeConfig的 截取
   */
  renderTruncate = width => (text, record, index) => {
    const encodeSource =
      record['encodeSource'] || record['encodeSource'] === 0
        ? String(record['encodeSource'])
        : '3'
    const showComponent = encodeSource !== '3'
    return (
      <div
        title={text}

      >
        {showComponent ? (
          <Switch
            checked={
              this.handleFilterEncodeType(record.cid) ? false : Boolean(text)
            }
            size="small"
            checkedChildren={<Icon type="check" />}
            unCheckedChildren={<Icon type="cross" />}
            disabled={this.handleFilterEncodeType(record.cid)}
            onChange={value =>
              this.handleChangeEncode(Number(value), record.cid, 'truncate')
            }
          />
        ) : (
          ''
        )}
      </div>
    )
  };
  /**
   * @description table的 Operating render
   */

  renderOperating = width => (text, record, index) => {
    const { outputListType } = this.state
    return (
      <div
        title={text}

      >
        <a
          onClick={e => this.handleDelEncode(e, record.cid)}
          disabled={outputListType === '0'}
        >
          <FormattedMessage id="app.common.delete" defaultMessage="删除" />
        </a>
      </div>
    )
  };
  render () {
    const { encodeList, locale } = this.props
    const {
      encodeOutputColumns,
      encodeSourceSelected,
      outputListType
    } = this.state
    let isChanged = false
    for (let cid in encodeOutputColumns) {
      if(encodeOutputColumns[cid].encodeSource === undefined) {
        encodeOutputColumns[cid].encodeSource = '3'
        isChanged = true
      }
    }
    if(isChanged) {
      const { tid } = this.props
      this.setState({ encodeOutputColumns })
      // 向父层传递数据
      this.handleSaveToReudx(tid, {
        encodeOutputColumns,
        outputListType
      })
    }
    const { loading, result } = encodeList
    const encodesAsyn = result && Object.values(result)
    const dataSource = Object.values(encodeOutputColumns)
    const localeMessage = intlMessage(locale)
    const placeholder = this.handlePlaceholder(localeMessage)
    const columns = [
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectHome.tabs.resource.line"
            defaultMessage="列名"
          />
        ),
        width: this.NomalTableWidth[0],
        dataIndex: 'columnName',
        key: 'columnName',
        render: this.renderComponent(this.renderNomal(this.NomalTableWidth[0]))
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectHome.tabs.resource.fieldType"
            defaultMessage="类型"
          />
        ),
        width: this.NomalTableWidth[1],
        dataIndex: 'dataType',
        key: 'dataType',
        render: this.renderComponent(this.renderNomal(this.NomalTableWidth[1]))
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectHome.tabs.resource.specialApprove"
            defaultMessage="特批不脱敏"
          />
        ),
        width: this.NomalTableWidth[2],
        dataIndex: 'specialApprove',
        key: 'specialApprove',
        render: this.renderComponent(
          this.renderSpecialApprove(this.NomalTableWidth[2])
        )
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectHome.tabs.resource.encodeNeed"
            defaultMessage="脱敏要求"
          />
        ),
        width: this.NomalTableWidth[3],
        dataIndex: 'encodeSource',
        key: 'encodeSource',
        render: this.renderComponent(
          this.renderEncodeNeed(this.NomalTableWidth[3])
        )
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectHome.tabs.resource.encodePlugin"
            defaultMessage="脱敏插件"
          />
        ),
        width: this.NomalTableWidth[4],
        dataIndex: 'encodePluginId',
        key: 'encodePluginId',
        render: this.renderComponent(
          this.renderEncodePlugin(this.NomalTableWidth[4])
        )
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectHome.tabs.resource.encodeType"
            defaultMessage="脱敏规则"
          />
        ),
        width: this.NomalTableWidth[5],
        dataIndex: 'encodeType',
        key: 'encodeType',
        render: this.renderComponent(
          this.renderEncodeType(this.NomalTableWidth[5])
        )
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectHome.tabs.resource.encodeParam"
            defaultMessage="脱敏参数"
          />
        ),
        width: this.NomalTableWidth[6],
        dataIndex: 'encodeParam',
        key: 'encodeParam',
        render: this.renderComponent(
          this.renderEncodeParam(this.NomalTableWidth[6])
        )
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectHome.tabs.resource.truncate"
            defaultMessage="截取"
          />
        ),
        width: this.NomalTableWidth[7],
        dataIndex: 'truncate',
        key: 'truncate',
        render: this.renderComponent(
          this.renderTruncate(this.NomalTableWidth[7])
        )
      },
      {
        title: (
          <FormattedMessage id="app.common.operate" defaultMessage="操作" />
        ),
        width: this.NomalTableWidth[7],
        render: this.renderComponent(
          this.renderOperating(this.NomalTableWidth[7])
        )
      }
    ]
    return (
      <div className={styles.encodeForm}>
        <div className={styles.form}>
          {/*<div className={styles.label}>选择输出类型：</div>*/}
          <div className={styles.formItem}>
            <FormattedMessage
              id="app.components.projectManage.projectTable.outputType"
              defaultMessage="选择输出类型"
            />：
            <RadioGroup
              onChange={this.handleRadioChange}
              value={outputListType}
            >
              <Radio value="0">
                <FormattedMessage
                  id="app.components.projectManage.projectTable.outputAll"
                  defaultMessage="输出所有列"
                />
                <Tooltip title={
                  <FormattedMessage
                    id="app.components.projectManage.projectTable.outputTip"
                    defaultMessage="与源端的输出列始终保持一致，如果源端发生了表结构变更，输出列也会随之改变"
                  />
                }>
                  <Icon
                    style={{ color: 'red', marginLeft: '4px' }}
                    type="question-circle-o"
                  />
                </Tooltip>
              </Radio>
              <Radio value="1">
                <FormattedMessage
                  id="app.components.projectManage.projectTable.outputFixed"
                  defaultMessage="输出固定列"
                />
              </Radio>
            </RadioGroup>
            {outputListType === '1' && (
              <div className={styles.encodeAdd}>
                <Select
                  className={styles.select}
                  mode="multiple"
                  placeholder={placeholder(
                    'app.components.projectManage.projectHome.tabs.resource.line'
                  )}
                  onChange={this.handleSelectEncodes}
                  notFoundContent={loading ? <Spin size="small" /> : null}
                  value={encodeSourceSelected}
                >
                  {encodesAsyn.map(item => (
                    <Option value={`${item.cid}`} key={`${item.cid}`}>
                      {item.encodeType ?
                        <span
                          title={item.encodeSource === 0 ? 'DBA脱敏' : '项目级脱敏'}
                        >
                          {item.columnName}
                          <Icon
                            type="lock"
                            style={item.encodeSource === 0 ? { color: "red"}: { color: "#00c1de"}}
                          />
                        </span>
                        : item.columnName}
                    </Option>
                  ))}
                </Select>
                <Button
                  type="primary"
                  className={styles.button}
                  onClick={this.handleAddEncode}
                >
                  <FormattedMessage
                    id="app.common.addColumn"
                    defaultMessage="添加列"
                  />
                </Button>
              </div>
            )}
          </div>
        </div>

        <div className={styles.table}>
          <Table
            rowKey={record => record.cid}
            size="small"
            dataSource={dataSource}
            columns={columns}
            loading={loading}
            pagination={false}
            scroll={{ x: true, y: 370 }}
          />
        </div>
      </div>
    )
  }
}

EncodeConfig.propTypes = {
  locale: PropTypes.any,
  tid: PropTypes.string,
  encodes: PropTypes.object,
  encodeList: PropTypes.object,
  encodeTypeList: PropTypes.object,
  onChange: PropTypes.func
}
