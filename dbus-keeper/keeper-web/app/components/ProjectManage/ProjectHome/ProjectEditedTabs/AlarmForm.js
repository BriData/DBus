/**
 * @author 戎晓伟
 * @description  报警设置
 */

import React, { PropTypes, Component } from 'react'
import { Form } from 'antd'
import { FormattedMessage } from 'react-intl'
import { intlMessage } from '@/app/i18n'
// 导入自定义组件
import AlamSelect from './common/AlamSelect'

const FormItem = Form.Item
@Form.create({ warppedComponentRef: true })
export default class AlarmForm extends Component {
  constructor (props) {
    super(props)
    this.formMessage = {
      en: {
        alarmPlaceholder: "Please enter '@' to select this project user",
        alarmMessage: 'Please enter the user who needs to alert the alarm'
      },
      zh: {
        alarmPlaceholder: "请输入'@'来选择本项目用户",
        alarmMessage: '请输入需要报警的用户'
      }
    }
  }
  /**
   * 校验当选中checked时报警用户是否为空
   */
  handleAlarmRule = (rule, value, callback) => {
    const localeMessage = intlMessage(this.props.locale, this.formMessage)
    if (value && value.checked && value.value.length < 1) {
      callback(localeMessage({ id: 'alarmMessage' }))
    } else {
      callback()
    }
  };
  handleChange =(key) => value => {
    const { setAlarm, onChangeAlarmFlag } = this.props
    const { validateFields } = this.props.form
    const alarm = {}
    alarm[key] = value
    validateFields((err, values) => {
      if (!err) {
        // 变为编辑状态
        onChangeAlarmFlag(true)
        // 存储数据
        setAlarm({...values, ...alarm})
      }
    })
  };
  render () {
    const { getFieldDecorator } = this.props.form
    const { user, alarm, locale } = this.props
    const formItemLayout = {
      labelCol: { span: 2 },
      wrapperCol: { span: 20 }
    }
    const userList = user
      ? user.map(item => ({ name: item.userName, email: item.email }))
      : []
    const localeMessage = intlMessage(locale, this.formMessage)
    return (
      <Form autoComplete="off" layout="horizontal" className="alarm">
        <FormItem
          label={
            <FormattedMessage
              id="app.components.projectManage.projectHome.tabs.alarm.table"
              defaultMessage="表结构变更报警"
            />
          }
          {...formItemLayout}
        >
          {getFieldDecorator('schemaChangeNotify', {
            initialValue: {
              checked: alarm.schemaChangeNotify.checked || false,
              value: alarm.schemaChangeNotify.value || []
            },
            rules: [
              {
                validator: this.handleAlarmRule
              }
            ]
          })(
            <AlamSelect
              userList={userList}
              onChange={this.handleChange('schemaChangeNotify')}
              placeholder={localeMessage({ id: 'alarmPlaceholder' })}
            />
          )}
        </FormItem>
        <FormItem
          label={
            <FormattedMessage
              id="app.components.projectManage.projectHome.tabs.alarm.receiving"
              defaultMessage="接收同步报警"
            />
          }
          {...formItemLayout}
        >
          {getFieldDecorator('slaveSyncDelayNotify', {
            initialValue: {
              checked: alarm.slaveSyncDelayNotify.checked || false,
              value: alarm.slaveSyncDelayNotify.value || []
            },
            rules: [
              {
                validator: this.handleAlarmRule
              }
            ]
          })(
            <AlamSelect
              userList={userList}
              onChange={this.handleChange('slaveSyncDelayNotify')}
              placeholder={localeMessage({ id: 'alarmPlaceholder' })}
            />
          )}
        </FormItem>

        <FormItem
          label={
            <FormattedMessage
              id="app.components.projectManage.projectHome.tabs.alarm.fullPull"
              defaultMessage="拉全量报警"
            />
          }
          {...formItemLayout}
        >
          {getFieldDecorator('fullpullNotify', {
            initialValue: {
              checked: alarm.fullpullNotify.checked || false,
              value: alarm.fullpullNotify.value || []
            },
            rules: [
              {
                validator: this.handleAlarmRule
              }
            ]
          })(
            <AlamSelect
              userList={userList}
              onChange={this.handleChange('fullpullNotify')}
              placeholder={localeMessage({ id: 'alarmPlaceholder' })}
            />
          )}
        </FormItem>
        <FormItem
          label={
            <FormattedMessage
              id="app.components.projectManage.projectHome.tabs.alarm.topology"
              defaultMessage="Topology延时报警"
            />
          }
          {...formItemLayout}
        >
          {getFieldDecorator('dataDelayNotify', {
            initialValue: {
              checked: alarm.dataDelayNotify.checked || false,
              value: alarm.dataDelayNotify.value || []
            },
            rules: [
              {
                validator: this.handleAlarmRule
              }
            ]
          })(
            <AlamSelect
              userList={userList}
              onChange={this.handleChange('dataDelayNotify')}
              placeholder={localeMessage({ id: 'alarmPlaceholder' })}
            />
          )}
        </FormItem>
      </Form>
    )
  }
}

AlarmForm.propTypes = {
  locale: PropTypes.any,
  form: PropTypes.object,
  user: PropTypes.array,
  alarm: PropTypes.object,
  setAlarm: PropTypes.func,
  onChangeAlarmFlag: PropTypes.func
}
