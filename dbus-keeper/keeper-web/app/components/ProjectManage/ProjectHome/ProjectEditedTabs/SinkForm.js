/**
 * @author 戎晓伟
 * @description  Sink信息设置
 */

import React, { PropTypes, Component } from 'react'
import { Table, Button, Input, Icon } from 'antd'
import { FormattedMessage } from 'react-intl'
import { fromJS } from 'immutable'
import { intlMessage } from '@/app/i18n'
// 导入样式
import styles from '../res/styles/index.less'

export default class SinkForm extends Component {
  constructor (props) {
    super(props)
    this.state = {
      nameVisible: false,
      bootStrapserverVisible: false
    }
    this.NomalTableWidth = ['20%', '20%', '50%']
    this.SelectTableWidth = ['20%', '20%', '50%']
    this.initParams = {
      pageNum: 1,
      pageSize: 5
    }
  }
  componentWillMount () {
    // 初始化查询
    this.handleSearch(this.initParams, true)
  }
  /**
   * @param key 传入一个key type:[Object String]  默认:空
   * @returns 返回一个随机字符串
   */
  handleRandom = key =>
    `${Math.random()
      .toString(32)
      .substr(3, 8)}${key || ''}`;
  /**
   * @param state [object String] 过滤弹出框 state
   * @description 控制自定义过滤弹出框的显示和隐藏
   */
  filterVisible = state => {
    const { sinkParams } = this.props
    return {
      filterDropdownVisible: this.state[`${state}Visible`],
      onFilterDropdownVisibleChange: visible => {
        let filterDropdownVisible = {}
        filterDropdownVisible[`${state}Visible`] = visible
        this.setState(filterDropdownVisible)
      },
      filterIcon: (
        <Icon
          type="filter"
          style={{ color: sinkParams && sinkParams[state] ? '#00c1de' : '#aaa' }}
        />
      )
    }
  };
  /**
   * @param params {key|value}
   * @description 过滤查询 如果value值为空或null删掉此参数并查询
   */
  handleFilterSearch = params => {
    const { sinkParams } = this.props
    const value = Object.values(params)[0]
    const key = Object.keys(params)[0]
    const newparams = fromJS(sinkParams).delete(key)
    // 关闭过滤框
    let filterDropdownVisible = {}
    filterDropdownVisible[`${key}Visible`] = false
    this.setState(filterDropdownVisible)

    this.handleSearch(
      value || value !== ''
        ? { ...sinkParams, ...params }
        : { ...newparams.toJS() },
      true
    )
  };
  /**
   * @param params 查询的参数 type:[Object Object]
   * @param boolean 是否将参数缓存起来 type:[Object Boolean]
   * @description 查询用户列表
   */
  handleSearch = (params, boolean) => {
    const { onSearchList, onSetParams } = this.props
    // 获取Sink列表
    onSearchList(params)
    if (boolean || boolean === undefined) {
      // 存储参数
      onSetParams(params)
    }
  };
  /**
   * @param page  传入的跳转页码  type:[Object Number]
   * @description table分页
   */
  handlePagination = page => {
    const { sinkParams } = this.props
    this.handleSearch({ ...sinkParams, pageNum: page }, true)
  };
  /**
   * @param key [object String] 已选用户对应的key值
   * @description 根据key值删除存储在redux中的Sink
   */
  handleDelSink = (e, key) => {
    e.preventDefault()
    const { setSink, sink } = this.props
    // 删除 用户
    const newSink = fromJS(sink).delete(`_${key}`)
    // 存储到redux
    setSink(newSink.toJS())
  };
  /**
   * @param record [object Object]  Sink数据
   * @description 添加Sink数据到redux里面
   */
  handleAddSink = (e, record) => {
    let newDataSource = {}
    const { setSink, sink } = this.props
    // 添加 _ 防止浏览器自动排序
    newDataSource[`_${record['id']}`] = { ...record }
    // 将生成的数据存储到redux中
    sink ? setSink({ ...newDataSource, ...sink }) : setSink(newDataSource)
  };
  /**
   * @deprecated input placeholder
   */
  handlePlaceholder = fun => id =>
    fun({
      id: 'app.common.mini.search.placeholder',
      valus: {
        name: fun({ id })
      }
    });
  // table render
  /**
   * @param render 传入一个render
   * @returns render 返回一个新的render
   * @description 统一处理render函数
   */
  renderComponent = render => (text, record, index) =>
    render(text, record, index);

  /**
   * @description table默认的render
   */
  renderNomal = width => (text, record, index) => (
    <div
      title={text}
      style={this.props.tableWidthStyle(width)}
      className={styles.ellipsis}
    >
      {text}
    </div>
  );
  /**
   * @description selectTable的 option render
   */
  renderOperating = (text, record, index) => (
    <div>
      {this.props.isUserRole ? (<FormattedMessage id="app.common.add" defaultMessage="添加"/>) : (
        <a onClick={e => this.handleAddSink(e, record)}>
          <FormattedMessage id="app.common.add" defaultMessage="添加"/>
        </a>
      )}
    </div>
  );
  /**
   * @description table的 option render
   */
  renderSelectOperating = (text, record, index) => (
    <div>
      {this.props.isUserRole ? (<FormattedMessage id="app.common.delete" defaultMessage="删除" />) : (
      <a onClick={e => this.handleDelSink(e, record.id)}>
        <FormattedMessage id="app.common.delete" defaultMessage="删除" />
      </a>
      )}
    </div>
  );
  render () {
    const {
      sink,
      locale,
      sinkList,
      selectedTableScrollY,
      errorFlag,
      isUserRole
    } = this.props
    const localeMessage = intlMessage(locale)
    const placeholder = this.handlePlaceholder(localeMessage)
    const { loading, result } = sinkList
    const dataSource = result && result.list
    const selectDataSource = sink ? Object.values(sink) : []
    const columns = [
      {
        title: <FormattedMessage id="app.common.name" defaultMessage="名称" />,
        width: this.NomalTableWidth[0],
        dataIndex: 'sinkName',
        key: 'sinkName',
        // 自定义过滤显隐
        ...this.filterVisible('sinkName'),
        filterDropdown: (
          <div className={styles.filterDropdown}>
            <Input
              placeholder={placeholder('app.common.user.name')}
              onPressEnter={e =>
                this.handleFilterSearch({
                  sinkName: e.target.value
                })
              }
            />
          </div>
        ),
        render: this.renderComponent(this.renderNomal(this.NomalTableWidth[0]))
      },
      {
        title: 'URL',
        width: this.NomalTableWidth[1],
        dataIndex: 'url',
        key: 'url',
        // 自定义过滤显隐
        ...this.filterVisible('url'),
        filterDropdown: (
          <div className={styles.filterDropdown}>
            <Input
              placeholder={placeholder('url')}
              onPressEnter={e =>
                this.handleFilterSearch({
                  url: e.target.value
                })
              }
            />
          </div>
        ),
        render: this.renderComponent(this.renderNomal(this.NomalTableWidth[1]))
      },
      {
        title: (
          <FormattedMessage id="app.common.description" defaultMessage="描述" />
        ),
        width: this.NomalTableWidth[2],
        dataIndex: 'sinkDesc',
        key: 'sinkDesc',
        render: this.renderComponent(this.renderNomal(this.NomalTableWidth[2]))
      },
      {
        title: (
          <FormattedMessage id="app.common.operate" defaultMessage="操作" />
        ),
        render: this.renderComponent(this.renderOperating)
      }
    ]
    const selectColumns = [
      {
        title: <FormattedMessage id="app.common.name" defaultMessage="名称" />,
        width: this.SelectTableWidth[0],
        dataIndex: 'sinkName',
        key: 'sinkName',
        render: this.renderComponent(this.renderNomal(this.SelectTableWidth[0]))
      },
      {
        title: 'URL',
        width: this.SelectTableWidth[1],
        dataIndex: 'url',
        key: 'url',
        render: this.renderComponent(this.renderNomal(this.SelectTableWidth[1]))
      },
      {
        title: (
          <FormattedMessage id="app.common.description" defaultMessage="描述" />
        ),
        width: this.SelectTableWidth[2],
        dataIndex: 'sinkDesc',
        key: 'sinkDesc',
        render: this.renderComponent(this.renderNomal(this.SelectTableWidth[2]))
      },
      {
        title: (
          <FormattedMessage id="app.common.operate" defaultMessage="操作" />
        ),
        render: this.renderComponent(this.renderSelectOperating)
      }
    ]
    const pagination = {
      showQuickJumper: true,
      current: (result && result.pageNum) || 1,
      pageSize: (result && result.pageSize) || 10,
      total: result && result.total,
      onChange: this.handlePagination
    }
    return (
      <div className={styles.tableLayout}>
        <div className={styles.table}>
          <Table
            size="small"
            rowKey={record => record.id}
            dataSource={dataSource}
            columns={columns}
            pagination={pagination}
            loading={loading}
          />
        </div>
        <div className={styles.table}>
          <h3
            className={
              dataSource && dataSource.length > 0
                ? styles.title
                : styles.titleNomal
            }
          >
            <FormattedMessage
              id="app.components.checkbox.selected"
              defaultMessage="已选Sink"
              values={{ value: 'Sink' }}
            />
            ：<span
              className={`${styles.info} ${errorFlag === 'sinkForm' &&
                styles.error}`}
            >
              （项目Sink为必选项，不能为空）
            </span>
          </h3>
          <Table
            size="small"
            rowKey={record => record.id}
            dataSource={selectDataSource}
            columns={selectColumns}
            pagination={false}
            scroll={{ y: selectedTableScrollY }}
          />
        </div>
      </div>
    )
  }
}

SinkForm.propTypes = {
  locale: PropTypes.any,
  sink: PropTypes.object,
  tableWidthStyle: PropTypes.func,
  selectedTableScrollY: PropTypes.number,
  errorFlag: PropTypes.string,
  setSink: PropTypes.func,
  sinkList: PropTypes.object,
  sinkParams: PropTypes.object,
  onSetParams: PropTypes.func,
  onSearchList: PropTypes.func
}
