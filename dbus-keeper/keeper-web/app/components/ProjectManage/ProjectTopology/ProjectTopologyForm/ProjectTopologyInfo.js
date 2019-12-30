/**
 * @author 戎晓伟
 * @description  基本信息设置
 */

import React, {PropTypes, Component} from 'react'
import {message, Form, Input, Select} from 'antd'
import Request from '@/app/utils/request'
import {FormattedMessage} from 'react-intl'
import {intlMessage} from '@/app/i18n'
import styles from '../res/styles/index.less'
// 导入API
import {IS_TOPOLOGY_TOPONAME_TRUE_API} from '@/app/containers/ProjectManage/api'

const FormItem = Form.Item
const Option = Select.Option
@Form.create({warppedComponentRef: true})
export default class ProjectTopologyInfo extends Component {
  constructor (props) {
    super(props)
    this.state = {
      reset: false,
      loading: false
    }
    this.formMessage = {
      en: {
        descriptionPlaceholder: 'description,Up to 150 words',
        projectNameMessage: 'The project name is required',
        principalMessage: 'The principal is required',
        topologyMessage:
          'The number of topology must be integers and 0<topologyNum<=100.'
      },
      zh: {
        descriptionPlaceholder: '项目描述，最多150字符',
        projectNameMessage: '项目名称为必填项',
        principalMessage: '负责人为必填项',
        topologyMessage: 'topology 个数必须为整数且 0<topologyNum<=100'
      }
    }
    this.formItemLayout = {
      labelCol: {span: 3},
      wrapperCol: {span: 19}
    }
  }

  componentWillMount = () => {
    const {topologyInfo, onGetJarVersions} = this.props
    // 获取Jar版本信息
    onGetJarVersions({version: topologyInfo.jarVersion})

    const {modalStatus, getTopologyTemplateApi} = this.props
    if (modalStatus === 'create') {
      Request(getTopologyTemplateApi, {
        method: 'get'
      })
        .then(res => {
          if (res && res.status === 0) {
            this.props.form.setFieldsValue({topoConfig: res.payload})
          } else {
            message.warn(res.message)
          }
        })
        .catch(error => {
          error.response.data && error.response.data.message
            ? message.error(error.response.data.message)
            : message.error(error.message)
        })
    }
  }
  /**
   * @description 获取Jar包列表
   */
  handleGetPackages = value => {
    const {onGetJarPackages} = this.props
    // 获取jar包
    onGetJarPackages({version: value})
  }

  /**
   * @description topoName blur 校验是否存在此TopoName
   */
  handleTopoNameBlur = e => {
    const API = IS_TOPOLOGY_TOPONAME_TRUE_API
    const value = e.target.value
    Request(`${API}/${value}`)
      .then(res => {
        if (res && res.status === 0) {
          if (res.payload > 0) {
            this.handleValidateError('topoName', value, 'Topo名称已存在')
          }
        } else {
          this.handleValidateError('topoName', value, 'Topo名称校验不通过')
        }
      })
      .catch(error => {
        console.error(error)
        this.handleValidateError('topoName', value, 'Topo名称校验不通过')
      })
  }
  /**
   * @param  name [object String] 控件名称
   * @param  value [object String] 控件的值
   * @param  error [object Stirng] 提示错误
   * @description 给控件添加错误信息
   */
  handleValidateError = (name, value, error) => {
    const {setFields} = this.props.form
    setFields({
      [name]: {
        value: value,
        errors: [new Error(`${error}`)]
      }
    })
  }
  /**
   * @deprecated input placeholder
   */
  handlePlaceholder = fun => id =>
    fun({
      id: 'app.components.input.placeholder',
      valus: {
        name: fun({id})
      }
    })

  render () {
    const {initialTopoNameText, initialTopoNamePrefix, initialAliasText, initialAliasPrefix} = this.props
    console.log(initialTopoNameText, initialTopoNamePrefix, initialAliasText, initialAliasPrefix)
    const {getFieldDecorator} = this.props.form
    const {topologyInfo, modalStatus, packages, versions} = this.props
    const localeMessage = intlMessage(this.props.locale, this.formMessage)
    const placeholder = this.handlePlaceholder(localeMessage)
    const topology = modalStatus === 'modify' ? topologyInfo : {}
    const versionsMap = Object.values(versions.result)
    const packagesMap = Object.values(packages.result)
    return (
      <Form autoComplete="off" layout="horizontal">
        <FormItem label={<FormattedMessage id="app.common.topo" defaultMessage="拓扑"/>} {...this.formItemLayout}>
          {getFieldDecorator('topoName', {
            initialValue: initialTopoNameText,
            rules: [
              {
                required: true,
                message: '仅支持字母、数字、下划线'
              },
              {
                pattern: /^[a-zA-Z0-9_]+$/,
                message: '仅支持字母、数字、下划线'
              }
            ]
          })(
            <Input
              type="text"
              disabled={modalStatus === 'modify'}
              addonBefore={initialTopoNamePrefix}
              onBlur={this.handleTopoNameBlur}
              placeholder={'Topo名称仅支持字母、数字、下划线'}
            />
          )}
        </FormItem>
        <FormItem label={<FormattedMessage id="app.common.alias" defaultMessage="拓扑别名"/>} {...this.formItemLayout}>
          {getFieldDecorator('alias', {
            initialValue: initialAliasText,
            rules: [
              {
                required: true,
                message: '仅支持字母、数字、下划线'
              },
              {
                pattern: /^[a-zA-Z0-9_]+$/,
                message: '仅支持字母、数字、下划线'
              }
            ]
          })(
            <Input
              type="text"
              addonBefore={initialAliasPrefix}
              placeholder={'别名仅支持字母、数字、下划线'}
            />
          )}
        </FormItem>
        {/* topoName,topoName,jarVersion,jarFilePath,status,topoComment,projectId */}
        <FormItem label={<FormattedMessage id="app.components.projectManage.projectTopology.table.config"
                                           defaultMessage="配置项"/>} {...this.formItemLayout}>
          {getFieldDecorator('topoConfig', {
            initialValue: (topology && topology.topoConfig) || ''
          })(<Input type="textarea" wrap='off' autosize={{minRows: 2, maxRows: 5}} placeholder={'请输入配置项'}/>)}
        </FormItem>
        <FormItem label={<FormattedMessage id="app.components.projectManage.projectTopology.table.jarVersion"
                                           defaultMessage="Jar版本"/>} {...this.formItemLayout}>
          {getFieldDecorator('jarVersion', {
            initialValue: topology && topology.jarVersion || versionsMap[0] || '暂无数据可选'
          })(
            <Select
              showSearch
              optionFilterProp='children'
              onChange={this.handleGetPackages}
            >
              {versionsMap.map(item => (
                <Option value={item} key={item || '0000'}>
                  {item}
                </Option>
              ))}
            </Select>
          )}
        </FormItem>
        <FormItem label={<FormattedMessage id="app.components.projectManage.projectTopology.table.jarName"
                                           defaultMessage="Jar包"/>} {...this.formItemLayout}>
          {getFieldDecorator('jarFilePath', {
            initialValue: topology && topology.jarFilePath || packagesMap[0] || '暂无数据可选'
          })(
            <Select
              showSearch
              optionFilterProp='children'
            >
              {packagesMap.map(item => (
                <Option value={item} key={item || '00001'}>
                  <div
                    title={item}
                    className={styles.ellipsis}
                  >
                    {item}
                  </div>
                </Option>
              ))}
            </Select>
          )}
        </FormItem>
        <FormItem label={<FormattedMessage id="app.common.user.backup" defaultMessage="备注"/>} {...this.formItemLayout}>
          {getFieldDecorator('topoComment', {
            initialValue: (topology && topology.topoComment) || ''
          })(<Input type="textarea" rows="2" placeholder={'请输入备注信息'}/>)}
        </FormItem>
      </Form>
    )
  }
}

ProjectTopologyInfo.propTypes = {
  locale: PropTypes.any,
  form: PropTypes.object,
  projectId: PropTypes.string,
  topologyInfo: PropTypes.object,
  modalStatus: PropTypes.string,
  packages: PropTypes.object,
  versions: PropTypes.object,
  onGetJarVersions: PropTypes.func,
  onGetJarPackages: PropTypes.func
}
