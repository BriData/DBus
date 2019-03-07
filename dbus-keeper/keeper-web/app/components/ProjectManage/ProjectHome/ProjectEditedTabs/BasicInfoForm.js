/**
 * @author 戎晓伟
 * @description  基本信息设置
 */

import React, { PropTypes, Component } from 'react'
import { Form, Input, Checkbox , DatePicker } from 'antd'
import moment from 'moment'
const MomentFormatString = "YYYY-MM-DD HH:mm:ss"
import { FormattedMessage } from 'react-intl'
import { intlMessage } from '@/app/i18n'
import dateFormat from 'dateformat'
const FormItem = Form.Item
@Form.create({ warppedComponentRef: true })
export default class BasicInfoForm extends Component {
  constructor (props) {
    super(props)
    this.formMessage = {
      en: {
        descriptionPlaceholder: 'description,Up to 150 words',
        projectNameMessage: 'The project code must be composed of uppercase ,lowercase letters, digit, _ , -',
        projectDisplayNameMessage: 'The project name can not be empty',
        ownerMessage: 'The owner is required',
        projectExpire: 'The expire time is illegal',
        topologyMessage:
          'The number of topology must be integers and 0<topologyNum<=100.'
      },
      zh: {
        descriptionPlaceholder: '项目描述，最多150字符',
        projectNameMessage: '项目代号须由大小写字母、数字、_、- 组成',
        projectDisplayNameMessage: '请填写项目名称',
        ownerMessage: '负责人为必填项',
        projectExpire: '请填写到期时间',
        topologyMessage: '拓扑个数必须为整数且 0<topologyNum<=100'
      }
    }
  }

  componentWillMount = () => {
    /**
     * 在新建的情况下，自动填写到期时间，设置到Redux里
     * 否则，如果用户不修改这一项，提交数据中将会没有
      */
    const {modalStatus} = this.props
    if (modalStatus === 'create') {
      const {setBasicInfo} = this.props
      const date = new Date()
      date.setFullYear(date.getFullYear() + 1)
      setBasicInfo({projectExpire: dateFormat(date, 'yyyy-mm-dd HH:MM:ss')})
    }
  }

  handleProjectExpireChange = value => {
    const { setBasicInfo, basicInfo } = this.props
    const getFieldsValue = this.props.form.getFieldsValue()
    // 存储数据
    setBasicInfo({...basicInfo, ...getFieldsValue, projectExpire: `${value.format(MomentFormatString)}`})
  }
  /**
   * @deprecated blur存储数据
   */
  handleBlur = () => {
    const { setBasicInfo, basicInfo } = this.props
    const getFieldsValue = this.props.form.getFieldsValue()
    delete getFieldsValue.projectExpire
    // 存储数据
    setBasicInfo({...basicInfo, ...getFieldsValue})
  };
  /**
   * @deprecated checkbox change 存储数据  // 暂时被抛弃
   */
  handleCheck = e => {
    const allowAdminManage = Number(e.target.checked)
    const { setBasicInfo } = this.props
    const getFieldsValue = this.props.form.getFieldsValue()
    delete getFieldsValue.projectExpire
    // 存储数据
    setBasicInfo({ ...getFieldsValue, allowAdminManage })
  };
  /**
   * @deprecated input placeholder
   */
  handlePlaceholder = fun => id =>
    fun({
      id: 'app.components.input.placeholder',
      valus: {
        name: fun({ id })
      }
    });

  render () {
    const { basicInfo, modalStatus , isUserRole} = this.props
    const { getFieldDecorator } = this.props.form
    const localeMessage = intlMessage(this.props.locale, this.formMessage)
    const placeholder = this.handlePlaceholder(localeMessage)
    const formItemLayout = {
      labelCol: { span: 2 },
      wrapperCol: { span: 12 }
    }
    console.info('basicInfo',basicInfo)
    return (
      <Form autoComplete="off" layout="horizontal">
        <FormItem
          label={
            <FormattedMessage
              id="app.components.projectManage.projectHome.tabs.basic.name"
              defaultMessage="项目名称"
            />
          }
          {...formItemLayout}
        >
          {getFieldDecorator('projectDisplayName', {
            initialValue: (basicInfo && basicInfo.projectDisplayName) || '',
            rules: [
              {
                required: true,
                message: localeMessage({ id: 'projectDisplayNameMessage' }),
              }
            ]
          })(
            <Input
              type="text"
              placeholder={localeMessage({ id: 'projectDisplayNameMessage' })}
              onBlur={this.handleBlur}
            />
          )}
        </FormItem>
        <FormItem
          label={
            <FormattedMessage
              id="app.components.projectManage.projectHome.tabs.basic.code"
              defaultMessage="项目代号"
            />
          }
          {...formItemLayout}
        >
          {getFieldDecorator('projectName', {
            initialValue: (basicInfo && basicInfo.projectName) || '',
            rules: [
              {
                required: true,
                pattern: /^[A-Za-z0-9_\-]+$/,
                message: localeMessage({ id: 'projectNameMessage' }),
                whitespace: true
              }
            ]
          })(
            <Input
              type="text"
              disabled={modalStatus === 'modify'}
              placeholder={localeMessage({ id: 'projectNameMessage' })}
              onBlur={this.handleBlur}
            />
          )}
        </FormItem>
        <FormItem
          label={
            <FormattedMessage
              id="app.components.projectManage.projectHome.tabs.basic.owner"
              defaultMessage="项目负责人"
            />
          }
          {...formItemLayout}
        >
          {getFieldDecorator('projectOwner', {
            initialValue: (basicInfo && basicInfo.projectOwner) || '',
            rules: [
              {
                required: true,
                message: localeMessage({ id: 'ownerMessage' }),
                whitespace: true
              }
            ]
          })(
            <Input
              type="text"
              placeholder={placeholder(
                'app.components.projectManage.projectHome.tabs.basic.owner'
              )}
              onBlur={this.handleBlur}
              disabled={isUserRole}
            />
          )}
        </FormItem>

        <FormItem
          label={
            <FormattedMessage
              id="app.components.projectManage.projectHome.tabs.basic.description"
              defaultMessage="项目描述"
            />
          }
          {...formItemLayout}
        >
          {getFieldDecorator('projectDesc', {
            initialValue: (basicInfo && basicInfo.projectDesc) || ''
          })(
            <Input
              type="textarea"
              rows="4"
              placeholder={placeholder('descriptionPlaceholder')}
              onBlur={this.handleBlur}
            />
          )}
        </FormItem>
        <FormItem
          label={
            <FormattedMessage
              id="app.components.projectManage.projectHome.tabs.basic.expire"
              defaultMessage="到期时间"
            />
          }
          {...formItemLayout}
        >
          {getFieldDecorator('projectExpire', {
            initialValue: (basicInfo && basicInfo.projectExpire && moment(basicInfo.projectExpire, "YYYY-MM-DD HH:mm:ss")) || moment().add(1,'years'),
            rules: [
              {
                required: true,
                message: localeMessage({ id: 'projectExpire' })
              }
            ]
          })(
            <DatePicker
              format={"YYYY-MM-DD HH:mm:ss"}
              showTime={true}
              onChange={this.handleProjectExpireChange}
              disabled={isUserRole}
            />
          )}
        </FormItem>
        <FormItem
          label={
            <FormattedMessage
              id="app.components.projectManage.projectHome.tabs.basic.topologyNum"
              defaultMessage="Topology个数"
            />
          }
          {...formItemLayout}
        >
          {getFieldDecorator('topologyNum', {
            initialValue: (basicInfo && basicInfo.topologyNum) || '',
            rules: [
              {
                required: true,
                pattern: /^([1-9][0-9]{0,1}|100)$/,
                message: localeMessage({ id: 'topologyMessage' })
              }
            ]
          })(
            <Input
              placeholder={placeholder(
                'app.components.projectManage.projectHome.tabs.basic.topologyNum'
              )}
              onBlur={this.handleBlur}
              disabled={isUserRole}
            />
          )}
        </FormItem>
        {/* <FormItem
          label={
            <FormattedMessage
              id="app.components.projectManage.projectHome.tabs.basic.help"
              defaultMessage="超管协管项目"
            />
          }
          {...formItemLayout}
        >
          {getFieldDecorator('allowAdminManage', {
            valuePropName: 'checked',
            initialValue: (basicInfo && basicInfo.allowAdminManage) || 1
          })(<Checkbox onChange={this.handleCheck} />)}
        </FormItem> */}
      </Form>
    )
  }
}

BasicInfoForm.propTypes = {
  locale: PropTypes.any,
  form: PropTypes.object,
  modalStatus: PropTypes.string,
  basicInfo: PropTypes.object,
  setBasicInfo: PropTypes.func
}
