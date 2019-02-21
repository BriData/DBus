import React, { PropTypes, Component } from 'react'
import { Table, Button, Input, Select, Switch, Icon, Spin } from 'antd'
import { FormattedMessage } from 'react-intl'
import { intlMessage } from '@/app/i18n'
import { fromJS } from 'immutable'

const Option = Select.Option
// 导入样式
import styles from '../../res/styles/index.less'

export default class EncodeConfig extends Component {
  constructor (props) {
    super(props)
    this.NomalTableWidth = ['8%', '8%', '10%', '22%', '22%', '20%', '5%', '5%']
    this.state = {
      encodeSourceList: {},
      encodeSourceSelected: []
    }
  }

  componentWillMount () {
    // 初始化赋值
    const { encodes, tid } = this.props
    const encodeSourceList =
      encodes && encodes[tid] ? fromJS(encodes[tid]) : fromJS({})
    this.setState({ encodeSourceList: encodeSourceList.toJS() })
  }
  /**
   * @description 将选中的encodeID过滤并添加到临时encodeSourceList中
   */
  handleAddEncode = () => {
    const { tid } = this.props
    const { encodeSourceSelected } = this.state
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
      encodeSourceList: {
        ...temporaryEncodeSource,
        ...this.state.encodeSourceList
      }
    })
    // 向父层传递数据
    this.handleSaveToReudx(tid, {
      ...temporaryEncodeSource,
      ...this.state.encodeSourceList
    })
  };

  /**
   * @param key [object String] 已选encode对应的key值
   * @description 删除临时encodeSourceList对应的encode
   */
  handleDelEncode = (e, key) => {
    const { tid } = this.props
    const { encodeSourceList } = this.state
    // 删除某项encode
    const temporaryEncodeSource = fromJS(encodeSourceList).delete(`_${key}`)
    this.setState({ encodeSourceList: temporaryEncodeSource.toJS() })
    // 向父层传递数据
    this.handleSaveToReudx(tid, temporaryEncodeSource.toJS())
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
    return !!(value && value['encodeType'])
  };

  /**
   * @param value [object String] 值
   * @param key  [object String]  需要匹配查询的ID
   * @param type [object String]  修改的属性
   * @description 修改encode属性并临时存储
   */
  handleChangeEncode = (value, key, type) => {
    const { tid } = this.props
    const { encodeSourceList } = this.state
    let temporaryEncodeSource = fromJS(encodeSourceList).setIn(
      [`_${key}`, type],
      value
    )
    if (type === 'encodePluginId') {
      temporaryEncodeSource = temporaryEncodeSource.setIn(
        [`_${key}`, 'encodeType'],
        null
      )
    }
    this.setState({ encodeSourceList: temporaryEncodeSource.toJS() })
    // 向父层传递数据
    this.handleSaveToReudx(tid, temporaryEncodeSource.toJS())
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
   * @param render 传入一个render
   * @returns render 返回一个新的render
   * @description 统一处理render函数
   */
  renderComponent = render => (text, record, index) =>
    render(text, record, index);

  /**
   * @description encodeConfig默认的render
   */
  renderNomal = width => (text, record, index) => (
    <div
      title={text}
      style={{ maxWidth: `${parseFloat(width) / 100 * 1000}px` }}
      className={styles.ellipsis}
    >
      {text}
    </div>
  );
  /**
   * @description 脱敏要求
   */
  renderEncodeNeed = width => (text, record, index) => (
    <span>
      {this.handleFilterEncodeType(record.cid) ? '统一脱敏' : '自定义脱敏'}
    </span>
  );

  /**
   * @description encodeConfig的脱敏插件
   */
  renderEncodePlugin = width => (text, record, index) => {
    console.info('record',record)
    const { locale, encodeTypeList } = this.props
    const { result, loading } = encodeTypeList
    const plugins = result.plugins || []
    const localeMessage = intlMessage(locale)
    const placeholder = this.handlePlaceholder(localeMessage)
    return (
      <div
        style={{ maxWidth: `${parseFloat(width) / 100 * 1000}px` }}
        className={styles.ellipsis}
      >
        <Select
          showSearch
          optionFilterProp='children'
          style={{ width: 170 }}
          notFoundContent={loading ? <Spin size="small" /> : null}
          placeholder={placeholder(
            'app.components.projectManage.projectHome.tabs.resource.encodePlugin'
          )}
          disabled={this.handleFilterEncodeType(record.cid)}
          onChange={value => {
            this.handleChangeEncode(value, record.cid, 'encodePluginId')
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
      </div>
    )
  };


  /**
   * @description encodeConfig的脱敏类型|脱敏规则
   */
  renderEncodeType = width => (text, record, index) => {
    const { locale } = this.props
    const { result, loading } = this.props.encodeTypeList
    const defaultEncoders = result.defaultEncoders || []
    const plugins = result.plugins || []
    const localeMessage = intlMessage(locale)
    const placeholder = this.handlePlaceholder(localeMessage)
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
        style={{ maxWidth: `${parseFloat(width) / 100 * 1000}px` }}
        className={styles.ellipsis}
      >
        <Select
          showSearch
          optionFilterProp='children'
          style={{ width: 180 }}
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
      </div>
    )
  };

  /**
   * @description encodeConfig的脱敏参数
   */
  renderEncodeParam = width => (text, record, index) => {
    const { locale } = this.props
    const localeMessage = intlMessage(locale)
    const handlePlaceholder = fun => id =>
      fun({
        id: 'app.components.input.placeholder',
        valus: {
          name: fun({ id })
        }
      })
    return (
      <div style={{ maxWidth: `${parseFloat(width) / 100 * 1000}px` }}>
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
      </div>
    )
  };

  /**
   * @description encodeConfig的 截取
   */
  renderTruncate = width => (text, record, index) => (
    <div
      title={text}
      style={{ maxWidth: `${parseFloat(width) / 100 * 1000}px` }}
    >
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
    </div>
  );
  /**
   * @description table的 Operating render
   */

  renderOperating = (text, record, index) => (
    <div>
      <a onClick={e => this.handleDelEncode(e, record.cid)}>
        <FormattedMessage id="app.common.delete" defaultMessage="删除" />
      </a>
    </div>
  );
  render () {
    const { encodeList, locale } = this.props
    const { encodeSourceList, encodeSourceSelected } = this.state
    const { loading, result } = encodeList
    const encodesAsyn = result && Object.values(result)
    const dataSource = Object.values(encodeSourceList)
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
            id="app.components.projectManage.projectHome.tabs.resource.encodeNeed"
            defaultMessage="脱敏要求"
          />
        ),
        width: this.NomalTableWidth[2],
        dataIndex: 'encodeNeed',
        key: 'encodeNeed',
        render: this.renderComponent(
          this.renderEncodeNeed(this.NomalTableWidth[2])
        )
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectHome.tabs.resource.encodePlugin"
            defaultMessage="脱敏插件"
          />
        ),
        width: this.NomalTableWidth[3],
        dataIndex: 'encodePluginId',
        key: 'encodePluginId',
        render: this.renderComponent(
          this.renderEncodePlugin(this.NomalTableWidth[3])
        )
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectHome.tabs.resource.encodeType"
            defaultMessage="脱敏规则"
          />
        ),
        width: this.NomalTableWidth[4],
        dataIndex: 'encodeType',
        key: 'encodeType',
        render: this.renderComponent(
          this.renderEncodeType(this.NomalTableWidth[4])
        )
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectHome.tabs.resource.encodeParam"
            defaultMessage="脱敏参数"
          />
        ),
        width: this.NomalTableWidth[5],
        dataIndex: 'encodeParam',
        key: 'encodeParam',
        render: this.renderComponent(
          this.renderEncodeParam(this.NomalTableWidth[5])
        )
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectHome.tabs.resource.truncate"
            defaultMessage="截取"
          />
        ),
        width: this.NomalTableWidth[6],
        dataIndex: 'truncate',
        key: 'truncate',
        render: this.renderComponent(
          this.renderTruncate(this.NomalTableWidth[6])
        )
      },
      {
        title: (
          <FormattedMessage id="app.common.operate" defaultMessage="操作" />
        ),
        render: this.renderComponent(this.renderOperating)
      }
    ]
    return (
      <div>
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
                  <span title='DBA脱敏'>
                    {item.columnName}
                    <Icon type="lock" style={{ color: "red"}} />
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
            <FormattedMessage id="app.common.add" defaultMessage="添加" />
          </Button>
        </div>
        <div className={styles.table}>
          <Table
            rowKey={record => record.cid}
            size="small"
            dataSource={dataSource}
            columns={columns}
            loading={loading}
            pagination={false}
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
