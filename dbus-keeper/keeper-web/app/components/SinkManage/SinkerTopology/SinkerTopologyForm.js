/**
 * @author 戎晓伟
 * @description  基本信息设置
 */

import React, {Component} from 'react'
import {Form, Input, message, Modal, Select} from 'antd'
import {FormattedMessage} from 'react-intl'
import {intlMessage} from '@/app/i18n'
// 导入样式
import {CREATE_SINKER_TOPOLOGY_API, UPDATE_SINKER_TOPOLOGY_API} from '@/app/containers/SinkManage/api'
import Request from '@/app/utils/request'

const FormItem = Form.Item
const Option = Select.Option
@Form.create({warppedComponentRef: true})
export default class SinkerTopologyForm extends Component {
  constructor(props) {
    super(props)
    this.formItemLayout = {
      labelCol: {span: 4},
      wrapperCol: {span: 18}
    }
  }

  /**
   * @deprecated 提交数据
   */
  handleSubmit = () => {
    const {
      sinkerInfo,
      modalStatus,
      onSearch,
      onClose,
      sinkerParams,
      jarList
    } = this.props
    const requestAPI = modalStatus === 'create' ? CREATE_SINKER_TOPOLOGY_API : UPDATE_SINKER_TOPOLOGY_API
    this.props.form.validateFieldsAndScroll((err, values) => {
      jarList.map(item => {
        if (item.id === values.jarPath) {
          values = {...values, jarId: item.id}
        }
      })
      if (!err) {
        const param = modalStatus === 'create'
          ? {
            ...values
          }
          : {
            ...sinkerInfo, ...values, id: values.id, updateTime: undefined
          }
        this.setState({loading: true})
        Request(requestAPI, {
          data: {
            ...param
          },
          method: 'post'
        })
          .then(res => {
            if (res && res.status === 0) {
              onClose()
              onSearch(sinkerParams)
            } else {
              message.warn(res.message)
            }
            this.setState({loading: false})
          })
          .catch(error => {
            error.response && error.response.data && error.response.data.message
              ? message.error(error.response.data.message)
              : message.error(error.message)
            this.setState({loading: false})
          })
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

  render() {
    const {getFieldDecorator} = this.props.form
    const {modalKey, visible, onClose, sinkerInfo, modalStatus, jarList} = this.props
    const localeMessage = intlMessage(this.props.locale, this.formMessage)
    const placeholder = this.handlePlaceholder(localeMessage)
    let disabled = modalStatus === 'modify' ? true : false
    return (
      <Modal
        key={modalKey}
        visible={visible}
        maskClosable={false}
        width={'800px'}
        style={{top: 60}}
        onCancel={() => onClose()}
        onOk={this.handleSubmit}
        confirmLoading={false}
        title={modalStatus === 'modify' ? '修改Sinker' : '添加Sinker'}
      >
        <Form autoComplete="off" layout="horizontal">
          {modalStatus === 'modify' && (<FormItem
            label={
              <FormattedMessage
                id="app.components.sinkManage.sinkerTopo.id"
                defaultMessage="id"
              />
            }
            {...this.formItemLayout}
          >
            {getFieldDecorator('id', {
              initialValue: sinkerInfo && sinkerInfo.id,
              rules: [
                {
                  required: false,
                  message: '请输入描述'
                }
              ]
            })(<Input disabled={true} placeholder={placeholder('app.components.sinkManage.sinkerTopo.id')}/>)}
          </FormItem>)
          }
          <FormItem
            label={
              <FormattedMessage
                id="app.components.sinkManage.sinkerTopo.sinkerName"
                defaultMessage="sinker名称"
              />
            }
            {...this.formItemLayout}
          >
            {getFieldDecorator('sinkerName', {
              initialValue: sinkerInfo && sinkerInfo.sinkerName,
              rules: [
                {
                  required: true,
                  message: '请输入名称'
                }
              ]
            })(
              <Input
                disabled={disabled}
                type="text"
                placeholder={'名称'}
                onBlur={this.handleBlur}
              />
            )}
          </FormItem>
          <FormItem label={<FormattedMessage id="app.components.projectManage.projectTopology.table.jarName"
                                             defaultMessage="Jar包"/>} {...this.formItemLayout}>
            {getFieldDecorator('jarPath', {
              initialValue: (sinkerInfo && sinkerInfo.jarPath),
              rules: [
                {
                  required: true,
                  message: '请选择jar包'
                }
              ]
            })(
              <Select
                showSearch
                optionFilterProp='children'
              >
                {jarList.map(item => (
                  <Option value={item.id} key={item.id}>
                    {item.path}
                  </Option>
                ))}
              </Select>
            )}
          </FormItem>
          {modalStatus === 'modify' && (
            <FormItem label={<FormattedMessage id="app.components.projectManage.projectTopology.table.config"
                                               defaultMessage="配置项"/>} {...this.formItemLayout}>
              {getFieldDecorator('sinkerConf', {
                initialValue: (sinkerInfo && sinkerInfo.sinkerConf) || ''
              })(<Input type="textarea" wrap='off' autosize={{minRows: 8, maxRows: 20}} placeholder={'请输入配置项'}/>)}
            </FormItem>)}
          <FormItem
            label={
              <FormattedMessage
                id="app.common.description"
                defaultMessage="描述"
              />
            }
            {...this.formItemLayout}
          >
            {getFieldDecorator('description', {
              initialValue: sinkerInfo && sinkerInfo.description,
              rules: [
                {
                  required: false,
                  message: '请输入描述'
                }
              ]
            })(<Input placeholder={placeholder('app.common.description')}/>)}
          </FormItem>
        </Form>
      </Modal>
    )
  }
}

SinkerTopologyForm.propTypes = {}
