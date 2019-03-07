/**
 * @author 戎晓伟
 * @description  User信息设置
 */

import React, { PropTypes, Component } from 'react'
import { Table, Dropdown, Icon, Menu, Input, Button } from 'antd'
import { FormattedMessage } from 'react-intl'
import { fromJS } from 'immutable'
import OperatingButton from '@/app/components/common/OperatingButton'
import { intlMessage } from '@/app/i18n'
// 导入样式
import styles from '../res/styles/index.less'

export default class UserForm extends Component {
  constructor (props) {
    super(props)
    this.state = {
      userNameVisible: false,
      emailVisible: false,
      mobileVisible: false
    }
    this.NomalTableWidth = ['25%', '25%', '25%', '21%']
    this.SelectTableWidth = ['16%', '15%', '20%', '30%', '10%']
    this.initParams = {
      pageNum: 1,
      pageSize: 8
    }
    // 临时存储选择的用户列表
    this.dataSource = {}
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
   * @param params 查询的参数 type:[Object Object]
   * @param boolean 是否将参数缓存起来 type:[Object Boolean]
   * @description 查询用户列表
   */
  handleSearch = (params, boolean) => {
    const { onSearchList, onSetParams } = this.props
    // 获取用户列表
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
    const { userParams } = this.props
    this.handleSearch({ ...userParams, pageNum: page }, true)
  };
  /**
   * @param item [object String] 用户类型
   * @description 添加用户到项目里面
   */
  handleAddUser = (item, record) => {
    let newDataSource = {}
    const { setUser, user, alarmFlag, modalStatus } = this.props
    // 添加 _ 防止浏览器自动排序
    newDataSource[`_${record['id']}`] = { ...record, userLevel: item.key }
    // 将生成的数据存储到redux中
    user ? setUser({ ...newDataSource, ...user }) : setUser(newDataSource)
    // 存储报警用户
    if (modalStatus === 'create' && !alarmFlag) {
      user
        ? this.handleSaveAlarms({ ...newDataSource, ...user })
        : this.handleSaveAlarms(newDataSource)
    }
  };
  /**
   * @description 存储报警用户列表
   */
  handleSaveAlarms = records => {
    const { setAlarm } = this.props
    const checked = true
    const value = Object.values(records).map(item => ({
      name: item.userName || item.email,
      email: item.email
    }))
    let alarms = {
      schemaChangeNotify: { checked, value },
      slaveSyncDelayNotify: { checked, value },
      fullpullNotify: { checked, value },
      dataDelayNotify: { checked, value }
    }
    // 存储报警用户组
    setAlarm(alarms)
  };

  /**
   * @param key [object String] 已选用户对应的key值
   * @description 根据key值删除存储在redux中的用户
   */
  handleDelUser = (e, key) => {
    e.preventDefault()
    const { setUser, user, modalStatus, alarmFlag } = this.props
    // 删除 用户
    const newUser = fromJS(user).delete(`_${key}`)
    // 存储到redux
    setUser(newUser.toJS())
    // 存储报警用户
    if (modalStatus === 'create' && !alarmFlag) {
      this.handleSaveAlarms(newUser.toJS())
    }
  };
  /**
   * @param state [object String] 过滤弹出框 state
   * @description 控制自定义过滤弹出框的显示和隐藏
   */
  filterVisible = state => {
    const { userParams } = this.props
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
          style={{ color: userParams && userParams[state] ? '#00c1de' : '#aaa' }}
        />
      )
    }
  };

  /**
   * @param params {key|value}
   * @description 过滤查询 如果value值为空或null删掉此参数并查询
   */
  handleFilterSearch = params => {
    const { userParams } = this.props
    const value = Object.values(params)[0]
    const key = Object.keys(params)[0]
    const newparams = fromJS(userParams).delete(key)
    // 关闭过滤框
    let filterDropdownVisible = {}
    filterDropdownVisible[`${key}Visible`] = false
    this.setState(filterDropdownVisible)

    this.handleSearch(
      value || value !== ''
        ? { ...userParams, ...params }
        : { ...newparams.toJS() },
      true
    )
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
   * @description table的 role render
   */
  renderRole = width => (text, record, index) => {
    switch (text) {
      case 'normal':
        return this.renderNomal(width)(
          <FormattedMessage
            id="app.components.projectManage.projectHome.tabs.user.role.ordinary"
            defaultMessage="普通用户"
          />,
          record,
          index
        )
      case 'readonly':
        return this.renderNomal(width)(
          <FormattedMessage
            id="app.components.projectManage.projectHome.tabs.user.role.readOnly"
            defaultMessage="只读用户"
          />,
          record,
          index
        )
      default:
        return this.renderNomal(width)(
          <FormattedMessage
            id="app.components.projectManage.projectHome.tabs.user.role.ordinary"
            defaultMessage="普通用户"
          />,
          record,
          index
        )
    }
  };
  /**
   * @description table的 option render
   */

  renderOperating = (text, record, index) => {
    // const menu = (
    //   <Menu onClick={item => this.handleAddUser(item, record)}>
    //     <Menu.Item key="normal">
    //       <FormattedMessage
    //         id="app.components.projectManage.projectHome.tabs.user.role.ordinary"
    //         defaultMessage="普通用户"
    //       />
    //     </Menu.Item>
    //     <Menu.Item key="readonly">
    //       <FormattedMessage
    //         id="app.components.projectManage.projectHome.tabs.user.role.readOnly"
    //         defaultMessage="只读用户"
    //       />
    //     </Menu.Item>
    //   </Menu>
    // )
    // return (
    //   <div>
    //     <Dropdown overlay={menu}>
    //       <a>
    //         <FormattedMessage id="app.common.add" defaultMessage="添加" />
    //         <Icon type="down" />
    //       </a>
    //     </Dropdown>
    //   </div>
    // )
    return (
      <div>
          <a href="javascript:void(0)" onClick={() => this.handleAddUser({key: 'normal'}, record)}>
            <FormattedMessage id="app.common.add" defaultMessage="添加" />
          </a>
      </div>
    )
  };
  /**
   * @description selectTable的 option render
   */
  renderSelectOperating = (text, record, index) => (
    <div>
      <OperatingButton onClick={e => this.handleDelUser(e, record.id)}>
        <FormattedMessage id="app.common.delete" defaultMessage="删除" />
      </OperatingButton>
    </div>
  );

  render () {
    const {
      user,
      locale,
      userList,
      selectedTableScrollY,
      errorFlag
    } = this.props
    const localeMessage = intlMessage(locale)
    const placeholder = this.handlePlaceholder(localeMessage)
    const columns = [
      {
        title: (
          <FormattedMessage id="app.common.user.name" defaultMessage="姓名" />
        ),
        width: this.NomalTableWidth[0],
        dataIndex: 'userName',
        key: 'userName',
        // 自定义过滤显隐
        ...this.filterVisible('userName'),
        filterDropdown: (
          <div className={styles.filterDropdown}>
            <Input
              placeholder={placeholder('app.common.user.name')}
              onPressEnter={e =>
                this.handleFilterSearch({
                  userName: e.target.value
                })
              }
            />
          </div>
        ),
        render: this.renderComponent(this.renderNomal(this.NomalTableWidth[0]))
      },
      {
        title: (
          <FormattedMessage id="app.common.user.email" defaultMessage="邮箱" />
        ),
        width: this.NomalTableWidth[1],
        dataIndex: 'email',
        key: 'email',
        // 自定义过滤显隐
        ...this.filterVisible('email'),
        filterDropdown: (
          <div className={styles.filterDropdown}>
            <Input
              placeholder={placeholder('app.common.user.email')}
              onPressEnter={e =>
                this.handleFilterSearch({
                  email: e.target.value
                })
              }
            />
          </div>
        ),
        render: this.renderComponent(this.renderNomal(this.NomalTableWidth[1]))
      },
      {
        title: (
          <FormattedMessage
            id="app.common.user.phone"
            defaultMessage="手机号"
          />
        ),
        width: this.NomalTableWidth[2],
        dataIndex: 'phoneNum',
        key: 'phoneNum',
        // 自定义过滤显隐
        ...this.filterVisible('phoneNum'),
        filterDropdown: (
          <div className={styles.filterDropdown}>
            <Input
              placeholder={placeholder('app.common.user.phone')}
              onPressEnter={e =>
                this.handleFilterSearch({
                  phoneNum: e.target.value
                })
              }
            />
          </div>
        ),
        render: this.renderComponent(this.renderNomal(this.NomalTableWidth[2]))
      },
      {
        title: (
          <FormattedMessage id="app.common.user.backup" defaultMessage="备注" />
        ),
        width: this.NomalTableWidth[3],
        dataIndex: 'notes',
        key: 'notes',
        render: this.renderComponent(this.renderNomal(this.NomalTableWidth[3]))
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
        title: (
          <FormattedMessage id="app.common.user.name" defaultMessage="姓名" />
        ),
        width: this.SelectTableWidth[0],
        dataIndex: 'userName',
        key: 'userName',
        render: this.renderComponent(this.renderNomal(this.SelectTableWidth[0]))
      },
      {
        title: (
          <FormattedMessage id="app.common.user.email" defaultMessage="邮箱" />
        ),
        width: this.SelectTableWidth[1],
        dataIndex: 'email',
        key: 'email',
        render: this.renderComponent(this.renderNomal(this.SelectTableWidth[1]))
      },
      {
        title: (
          <FormattedMessage
            id="app.common.user.phone"
            defaultMessage="手机号"
          />
        ),
        width: this.SelectTableWidth[2],
        dataIndex: 'phoneNum',
        key: 'phoneNum',
        render: this.renderComponent(this.renderNomal(this.SelectTableWidth[2]))
      },
      {
        title: (
          <FormattedMessage id="app.common.user.backup" defaultMessage="备注" />
        ),
        width: this.SelectTableWidth[3],
        dataIndex: 'notes',
        key: 'notes',
        render: this.renderComponent(this.renderNomal(this.SelectTableWidth[3]))
      },
      {
        title: (
          <FormattedMessage
            id="app.common.user.role"
            defaultMessage="用户角色"
          />
        ),
        width: this.SelectTableWidth[4],
        dataIndex: 'userLevel',
        key: 'userLevel',
        render: this.renderComponent(this.renderRole(this.SelectTableWidth[4]))
      },
      {
        title: (
          <FormattedMessage id="app.common.operate" defaultMessage="操作" />
        ),
        render: this.renderComponent(this.renderSelectOperating)
      }
    ]
    const { loading, result } = userList
    const dataSource = result && result.list
    const selectDataSource = user ? Object.values(user) : []
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
            loading={loading}
            pagination={pagination}
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
              defaultMessage="已选用户"
              values={{ value: locale === 'en' ? 'user' : '用户' }}
            />
            ：<span
              className={`${styles.info} ${errorFlag === 'userForm' &&
                styles.error}`}
            >
              （项目用户为必选项，不能为空）
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

UserForm.propTypes = {
  locale: PropTypes.any,
  user: PropTypes.object,
  modalStatus: PropTypes.string,
  alarmFlag: PropTypes.bool,
  errorFlag: PropTypes.string,
  tableWidthStyle: PropTypes.func,
  selectedTableScrollY: PropTypes.number,
  userList: PropTypes.object,
  userParams: PropTypes.object,
  setUser: PropTypes.func,
  setAlarm: PropTypes.func,
  onSearchList: PropTypes.func,
  onSetParams: PropTypes.func
}
