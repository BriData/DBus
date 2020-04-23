import React, { PropTypes, Component } from 'react'
import { Modal, Form, Select, Input, Button, message,Table } from 'antd'
import { FormattedMessage } from 'react-intl'
import OperatingButton from '@/app/components/common/OperatingButton'

// 导入样式
import styles from './res/styles/index.less'
import Request from "@/app/utils/request";

const FormItem = Form.Item
const Option = Select.Option
const TextArea = Input.TextArea

@Form.create()
export default class DataTableManageModifyModal extends Component {
  constructor (props) {
    super(props)
    this.state = {
    }
  }

  handleSubmit = () => {
    const {updateApi} = this.props
    const {onClose, onRequest} = this.props
    this.props.form.validateFieldsAndScroll((err, values) => {
      if (!err) {
        onRequest({
          api: updateApi,
          data: {
            ...values,
            createTime: undefined
          },
          method: 'post',
          callback: onClose,
        })
      }
    })
  }

  render () {
    const { getFieldDecorator } = this.props.form
    const {key, visible, tableInfo, onClose} = this.props
    const formItemLayout = {
      labelCol: {
        xs: { span: 5 },
        sm: { span: 6 }
      },
      wrapperCol: {
        xs: { span: 19 },
        sm: { span: 12 }
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
            id="app.components.resourceManage.dataTable.modifyTable"
            defaultMessage="修改表"
          />}
          onCancel={onClose}
          onOk={this.handleSubmit}
        >
          <Form autoComplete="off" onKeyUp={e => {
            e.keyCode === 13 && this.handleSubmit()
          }} className="data-source-modify-form">
            <FormItem label="ID" {...formItemLayout}>
              {getFieldDecorator('id', {
                initialValue: tableInfo.id,
              })(<Input disabled={true} size="large" type="text" />)}
            </FormItem>
            <FormItem label={<FormattedMessage
              id="app.components.resourceManage.dataSourceName"
              defaultMessage="数据源名称"
            />} {...formItemLayout}>
              {getFieldDecorator('dsName', {
                initialValue: tableInfo.dsName,
              })(<Input disabled={true} size="large" type="text" />)}
            </FormItem>
            <FormItem label={<FormattedMessage
              id="app.components.resourceManage.dataSchemaName"
              defaultMessage="Schema名称"
            />} {...formItemLayout}>
              {getFieldDecorator('schemaName', {
                initialValue: tableInfo.schemaName,
              })(<Input disabled={true} size="large" type="text" />)}
            </FormItem>
            <FormItem label={<FormattedMessage
              id="app.components.resourceManage.dataTableName"
              defaultMessage="表名"
            />} {...formItemLayout}>
              {getFieldDecorator('tableName', {
                initialValue: tableInfo.tableName,
              })(<Input disabled={true} size="large" type="text" />)}
            </FormItem>
            <FormItem label={<FormattedMessage
              id="app.components.resourceManage.dataTableNameRegex"
              defaultMessage="表名正则"
            />} {...formItemLayout}>
              {getFieldDecorator('physicalTableRegex', {
                initialValue:tableInfo.physicalTableRegex,
                rules: [
                  {
                    required: true,
                    message: '不能为空'
                  }
                ]
              })(<Input size="large" type="text" />)}
            </FormItem>
            {/*<FormItem label={<FormattedMessage*/}
              {/*id="app.components.resourceManage.dataTableNameAlias"*/}
              {/*defaultMessage="模板表"*/}
            {/*/>} {...formItemLayout}>*/}
              {/*{getFieldDecorator('tableNameAlias', {*/}
                {/*initialValue: tableInfo.tableNameAlias*/}
              {/*})(<Input size="large" type="text"/>)}*/}
            {/*</FormItem>*/}
            <FormItem label={<FormattedMessage
              id="app.common.description"
              defaultMessage="描述"
            />} {...formItemLayout}>
              {getFieldDecorator('description', {
                initialValue: tableInfo.description,
              })(<Input size="large" type="text" />)}
            </FormItem>

            <FormItem
              label={<FormattedMessage
                id="app.components.resourceManage.dataTable.beforeUpdate"
                defaultMessage="输出before"
              />} {...formItemLayout}
            >
              {getFieldDecorator('outputBeforeUpdateFlg', {
                initialValue:`${tableInfo.outputBeforeUpdateFlg}`
              })(
                <Select
                  showSearch
                  optionFilterProp='children'
                  className={styles.select}
                  placeholder="Select status"
                >
                  <Option value="1" key="1">Yes</Option>
                  <Option value="0" key="0">No</Option>
                </Select>
              )}
            </FormItem>
            <FormItem label="分片列" {...formItemLayout}>
              {getFieldDecorator('split_col', {
                initialValue: tableInfo.fullpullCol,
              })(<Input size="large" type="text" />)}
            </FormItem>
            <FormItem label="分片大小" {...formItemLayout}>
              {getFieldDecorator('fullpullSplitShardSize', {
                initialValue: tableInfo.fullpullSplitShardSize,
              })(<Input size="large" type="text" />)}
            </FormItem>
            <FormItem label="分片类型" {...formItemLayout}>
              {getFieldDecorator('fullpullSplitStyle', {
                initialValue: tableInfo.fullpullSplitStyle,
              })(<Input size="large" type="text" />)}
            </FormItem>
            {tableInfo.dsType === 'mongo' ?
              (
                <FormItem
                  label={<FormattedMessage
                    id="app.components.resourceManage.dataSource.isExpand"
                    defaultMessage="是否展开"
                  />} {...formItemLayout}
                >
                  {getFieldDecorator('isOpen', {
                    initialValue: tableInfo.isOpen ? tableInfo.isOpen : 0
                  })(
                    <Select
                      showSearch
                      optionFilterProp='children'
                      className={styles.select}
                      placeholder="Select status"
                    >
                      <Option value={1} key="1">Yes</Option>
                      <Option value={0} key="0">No</Option>
                    </Select>
                  )}
                </FormItem>
              ) : (
                <span/>
              )
            }
            {tableInfo.dsType === 'mongo' ?
              (
                <FormItem
                  label={<FormattedMessage
                    id="app.components.resourceManage.dataSource.isAutoComplete"
                    defaultMessage="是否自动补全"
                  />} {...formItemLayout}
                >
                  {getFieldDecorator('isAutoComplete', {
                    initialValue: tableInfo.isAutoComplete || false
                  })(
                    <Select
                      showSearch
                      optionFilterProp='children'
                      className={styles.select}
                      placeholder="Select status"
                    >
                      <Option value={true} key="yes">Yes</Option>
                      <Option value={false} key="no">No</Option>
                    </Select>
                  )}
                </FormItem>
              ) : (
                <span/>
              )
            }
          </Form>
        </Modal>
      </div>
    )
  }
}

DataTableManageModifyModal.propTypes = {
}
