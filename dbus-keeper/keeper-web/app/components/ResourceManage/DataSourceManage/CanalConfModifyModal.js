import React, {Component} from 'react'
import {Button, Form, Input, message, Modal, Spin} from 'antd'
import {FormattedMessage} from 'react-intl'
// 导入样式
import styles from './res/styles/index.less'
import Request from "@/app/utils/request";
import {
  DATA_SOURCE_SET_CANAL_CONF_API,
  DATA_SOURCE_AUTO_ADD_OGG_CANAL_LINE_API
} from '@/app/containers/ResourceManage/api'

const FormItem = Form.Item

@Form.create()
export default class CanalConfModifyModal extends Component {
  constructor(props) {
    super(props)
    this.state = {}
  }

  handleSubmit = (action) => {
    const {onClose} = this.props
    this.props.form.validateFieldsAndScroll((err, values) => {
      if (!err) {
        Request(DATA_SOURCE_SET_CANAL_CONF_API, {
          data: {
            ...values
          },
          method: 'post'
        })
          .then(res => {
            if (res && res.status === 0) {
              if (action === 'onlySave') {
                message.success(res.message)
                onClose()
              } else {
                this.handleAutoDeploy(values)
                onClose()
              }
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
    })
  }
  handleAutoDeploy = (values) => {
    Request(DATA_SOURCE_AUTO_ADD_OGG_CANAL_LINE_API, {
      params: {
        dsName: values.dsName,
        canalUser: values.canalUser,
        canalPass: values.canalPass,
      },
      method: 'get'
    })
  }

  render() {
    const {getFieldDecorator} = this.props.form
    const {key, visible, oggCanalConf, onClose} = this.props
    const {result} = oggCanalConf;
    const formItemLayout = {
      labelCol: {
        xs: {span: 5},
        sm: {span: 6}
      },
      wrapperCol: {
        xs: {span: 19},
        sm: {span: 12}
      }
    }
    return (
      <div className={styles.table}>
        <Modal
          className="top-modal"
          key={key}
          visible={visible}
          maskClosable={false}
          width={1000}
          title={<FormattedMessage
            id="app.components.resourceManage.dataSource.modifyCanalConf"
            defaultMessage="canal配置修改"
          />}
          onCancel={onClose}
          footer={[
            <Button onClick={onClose}> 返 回 </Button>,
            <Button type="primary" onClick={() => this.handleSubmit('onlySave')}> 保存canal配置 </Button>,
            <Button type="primary" onClick={() => this.handleSubmit('saveAndSubmit')}> 保存并自动部署canal </Button>,
          ]}
        >
          <Spin spinning={oggCanalConf.loading} tip="正在加载数据中...">
            {!oggCanalConf.loading ? (
              <Form autoComplete="off" className="data-source-modify-form">
                <FormItem label={<FormattedMessage
                  id="app.components.resourceManage.dataSourceName"
                  defaultMessage="数据源名称"
                />} {...formItemLayout}>
                  {getFieldDecorator('dsName', {
                    initialValue: result.dsName,
                  })(<Input disabled={true} size="default" type="text"/>)}
                </FormItem>
                <FormItem label="deployHost" {...formItemLayout}>
                  {getFieldDecorator('host', {
                    initialValue: result.host,
                  })(<Input size="default" type="text"/>)}
                </FormItem>
                <FormItem label="deployPath" {...formItemLayout}>
                  {getFieldDecorator('canalPath', {
                    initialValue: result.canalPath,
                  })(<Input size="default" type="text"/>)}
                </FormItem>
                <FormItem label="slaveAddress" {...formItemLayout}>
                  {getFieldDecorator('canalAdd', {
                    initialValue: result.canalAdd,
                  })(<Input size="default" type="text"/>)}
                </FormItem>
                <FormItem label="canalSlaveId" {...formItemLayout}>
                  {getFieldDecorator('slaveId', {
                    initialValue: result.slaveId,
                  })(<Input size="default" type="text"/>)}
                </FormItem>
                <FormItem label="canalUser" {...formItemLayout}>
                  {getFieldDecorator('canalUser', {
                    initialValue: result.canalUser,
                  })(<Input size="default" type="text"/>)}
                </FormItem>
                <FormItem label="canalPass" {...formItemLayout}>
                  {getFieldDecorator('canalPass', {
                    initialValue: result.canalPass,
                  })(<Input size="default" type="password"/>)}
                </FormItem>
              </Form>
            ) : (
              <div style={{height: '378px'}}/>
            )}
          </Spin>
        </Modal>
      </div>
    )
  }
}

CanalConfModifyModal.propTypes = {}
