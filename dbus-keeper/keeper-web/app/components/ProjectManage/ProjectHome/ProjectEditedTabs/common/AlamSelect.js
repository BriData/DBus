/**
 * @author 戎晓伟
 * @param value [object Object] {checked,value} [{是否开启报警,报警用户}]
 * @param onChange  [object Function] 查询方法
 * @param userList  [object Array] 报警用户列表
 * @description  报警添加用户组件
 */
import React, { PropTypes, Component } from 'react'
import debounce from 'lodash.debounce'
import { Select, Spin, Switch, Icon, Row, Col, message } from 'antd'
import { fromJS, is } from 'immutable'
// 导入查询用户API
import Request from '@/app/utils/request'
import { SEARCH_USER_API } from '@/app/containers/ProjectManage/api'

// 导入样式
import styles from '../../res/styles/index.less'

const Option = Select.Option

export default class AlarmSelect extends Component {
  constructor (props) {
    super(props)
    this.lastFetchId = 0
    this.handleFetchUser = debounce(this.handleFetchUser, 500)
    this.state = {
      checked: false,
      value: [],
      userList: [],
      fetching: false,
      // 如果是内部state改变过后则不接受外部数据修改
      alarmFlag: true
    }
    this.initParams = {
      pageNum: 1,
      pageSize: 100
    }
  }
  componentWillMount () {
    const { value, checked } = this.props.value
      ? this.props.value
      : { value: [], checked: false }
    this.setState({
      checked,
      value: value.map(item => ({
        key: item.email,
        label: (
          <span title={item.email} name={item.name}>
            {item.name}
          </span>
        )
      }))
    })
  }
  componentWillReceiveProps (nextProps) {
    const { checked, value, alarmFlag } = this.state
    const values = this.handleFormatValue(checked, value).value
    const nextValues = nextProps.value.value
    const nextChecked = nextProps.value.checked
    // 如果传入的数据不同则重新赋值渲染
    if (alarmFlag && !is(fromJS(values), fromJS(nextValues))) {
      this.setState({
        checked: nextChecked,
        value: nextValues.map(item => this.handleFormatUser(item))
      })
    }
  }

  /**
   * @description 是否开启报警
   */
  handleSwitchChange = checked => {
    const { onChange } = this.props
    const value = this.handleFormatValue(checked, this.state.value)
    this.setState({ checked, alarmFlag: false })
    onChange(value)
  };
  /**
   * @description 异步加载用户
   */
  handleFetchUser = value => {
    this.lastFetchId += 1
    const fetchId = this.lastFetchId
    this.setState({ fetching: true })
    if (value === '@') {
      const { userList } = this.props
      const data = userList.map(user => ({
        label: (
          <span title={user.email} name={user.name}>
            {user.name}
          </span>
        ),
        key: user.email,
        fetching: false
      }))
      this.setState({ userList: data })
    } else {
      // 用户查询
      // params {pageNum,pageSize,sortby,order,userName,email,phoneNum}
      Request(SEARCH_USER_API, {
        params: { ...this.initParams, userName: value },
        method: 'get'
      })
        .then(res => {
          if (fetchId !== this.lastFetchId) {
            // for fetch callback order
            return
          }
          if (res && res.status === 0) {
            if (res.payload) {
              const userList =
                res.payload &&
                res.payload.list.map(user => ({
                  label: (
                    <span title={user.email} name={user.userName}>
                      {user.userName}
                    </span>
                  ),
                  key: user.email,
                  fetching: false
                }))
              this.setState({ userList })
            }
          }
        })
        .catch(error => {
          error.response.data && error.response.data.message
            ? message.error(error.response.data.message)
            : message.error(error.message)
        })
    }
  };
  /**
   *@description select onChange
   */
  handleChange = value => {
    // 过滤
    const filterValues = this.handleFilter(value)
    if (filterValues.length !== value.length) {
      message.warn('报警通知邮箱不存在', 2)
    }
    this.setState({
      value: filterValues,
      userList: [],
      fetching: false,
      alarmFlag: false
    })
  };
  /**
   *@param value  [object Array] {key,label}
   *@description 过滤掉value中key不为邮箱的用户
   */
  handleFilter = value => {
    const reg = /^[a-zA-Z0-9_.-]+@[a-zA-Z0-9-]+(\.[a-zA-Z0-9-]+)*\.[a-zA-Z0-9]{2,6}$/
    return value.filter(item => reg.test(item.key))
  };
  /**
   *@description select onBlur 存储数据
   */
  handleBlur = e => {
    const { onChange } = this.props
    const value = this.handleFormatValue(this.state.checked, this.state.value)
    this.setState({
      userList: [],
      fetching: false
    })
    onChange(value)
  };
  /**
   * @param checked [object Boolean] 是否开启报警
   * @param value select 选中的用户
   * @description 格式化数据
   */
  handleFormatValue = (checked, value) => {
    const newEditorState = value.map(item => ({
      email: item.key,
      name: (item.label.props && item.label.props.name) || item.label
    }))
    return {
      checked,
      value: newEditorState
    }
  };
  /**
   * @param user [object object] {email,name}
   * @description  格式化用户
   */
  handleFormatUser = user => ({
    label: (
      <span title={user.email} name={user.name}>
        {user.name}
      </span>
    ),
    key: user.email
  });
  render () {
    const { fetching, userList, value, checked } = this.state
    const {placeholder} = this.props
    return (
      <Row>
        <div className={styles.layoutLeft}>
          <Switch
            checkedChildren={<Icon type="check" />}
            unCheckedChildren={<Icon type="cross" />}
            checked={checked}
            size='small'
            onChange={this.handleSwitchChange}
          />
        </div>
        <div className={styles.layoutRight}>
          <Select
            mode="tags"
            labelInValue
            disabled={!checked}
            value={value}
            placeholder={placeholder || ''}
            notFoundContent={fetching ? <Spin size="small" /> : null}
            filterOption={false}
            onSearch={this.handleFetchUser}
            onChange={this.handleChange}
            onBlur={this.handleBlur}
            style={{ width: '100%' }}
            className="mention-select"
          >
            {userList.map(d => <Option key={d.key}>{d.label}</Option>)}
          </Select>
        </div>
      </Row>
    )
  }
}

AlarmSelect.propTypes = {
  value: PropTypes.object,
  onChange: PropTypes.func,
  placeholder: PropTypes.string,
  userList: PropTypes.array
}
