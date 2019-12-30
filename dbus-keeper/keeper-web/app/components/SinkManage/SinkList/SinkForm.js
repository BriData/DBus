/**
 * @author 戎晓伟
 * @description  基本信息设置
 */

import React, {Component, PropTypes} from 'react'
import {Form, Input, message, Modal, Select} from 'antd'
import {FormattedMessage} from 'react-intl'
import {intlMessage} from '@/app/i18n'
// 导入样式
import {CREATE_SINK_API, UPDATE_SINK_API} from '@/app/containers/SinkManage/api'
import Request from '@/app/utils/request'

const FormItem = Form.Item
const Option = Select.Option
@Form.create({warppedComponentRef: true})
export default class SinkForm extends Component {
  constructor (props) {
    super(props)
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
    this.dataTypeGroup = [
      {value: 'user', text: 'User'},
      {value: 'app', text: 'App'},
      {value: 'admin', text: 'Admin'}
    ]
    this.formItemLayout = {
      labelCol: {span: 4},
      wrapperCol: {span: 12}
    }
  }

  /**
   * @deprecated 提交数据
   */
  handleSubmit = () => {
    const {
      sinkInfo,
      modalStatus,
      onSearch,
      onCloseModal,
      sinkParams
    } = this.props
    const requestAPI =
      modalStatus === 'create' ? CREATE_SINK_API : UPDATE_SINK_API
    this.props.form.validateFieldsAndScroll((err, values) => {
      if (!err) {
        const param =
          modalStatus === 'create'
            ? {
              ...values
            }
            : {...sinkInfo, ...values, updateTime: undefined}
        this.setState({loading: true})
        Request(requestAPI, {
          data: {
            ...param
          },
          method: 'post'
        })
          .then(res => {
            if (res && res.status === 0) {
              onCloseModal(false)
              onSearch(sinkParams)
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

  render () {
    const {getFieldDecorator} = this.props.form
    const {modalKey, visible, onCloseModal, sinkInfo, modalStatus} = this.props
    const localeMessage = intlMessage(this.props.locale, this.formMessage)
    const placeholder = this.handlePlaceholder(localeMessage)
    return (
      <Modal
        key={modalKey}
        visible={visible}
        maskClosable={false}
        width={'800px'}
        style={{top: 60}}
        onCancel={() => onCloseModal(false)}
        onOk={this.handleSubmit}
        confirmLoading={false}
        title={modalStatus === 'modify' ? <FormattedMessage
          id="app.components.sinkManage.modifySink"
          defaultMessage="修改Sink"
        /> : <FormattedMessage
          id="app.components.sinkManage.addSink"
          defaultMessage="添加Sink"
        />}
      >
        <Form autoComplete="off" layout="horizontal">
          <FormItem
            label={
              <FormattedMessage
                id="app.common.name"
                defaultMessage="名称"
              />
            }
            {...this.formItemLayout}
          >
            {getFieldDecorator('sinkName', {
              initialValue: (sinkInfo && sinkInfo.sinkName) || 'kafka',
              rules: [
                {
                  required: true,
                  message: '请输入名称'
                }
              ]
            })(
              <Input
                type="text"
                placeholder={'名称'}
                onBlur={this.handleBlur}
              />
            )}
          </FormItem>
          <FormItem
            label={
              <FormattedMessage
                id="app.components.configCenter.globalConfig.bootstrapServers"
                defaultMessage="Kafka 服务器"
              />
            }
            {...this.formItemLayout}
          >
            {getFieldDecorator('url', {
              initialValue: (sinkInfo && sinkInfo.url) || 'localhost:9092',
              rules: [
                {
                  required: true,
                  message: '请输入正确的Bootstrap Servers'
                }
              ]
            })(
              <Input
                type="text"
                placeholder={placeholder('Bootstrap Servers')}
              />
            )}
          </FormItem>
          <FormItem label={
            <FormattedMessage
              id="app.common.version"
              defaultMessage="版本"
            />
          } {...this.formItemLayout}>
            {getFieldDecorator('sinkType', {
              initialValue: (sinkInfo && sinkInfo.sinkType) || '0.10.0.0',
              rules: [
                {
                  required: true,
                  message: '请输入version'
                }
              ]
            })(
              <Input
                type="text"
                placeholder={placeholder('version')}
              />
            )}
          </FormItem>
          <FormItem
            label={
              <FormattedMessage
                id="app.common.description"
                defaultMessage="描述"
              />
            }
            {...this.formItemLayout}
          >
            {getFieldDecorator('sinkDesc', {
              initialValue: (sinkInfo && sinkInfo.sinkDesc) || '描述信息',
              rules: [
                {
                  required: true,
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

SinkForm.propTypes = {
  locale: PropTypes.any,
  form: PropTypes.object,
  sink: PropTypes.object,
  modalStatus: PropTypes.string,
  visibal: PropTypes.bool,
  onCloseModal: PropTypes.func
}
