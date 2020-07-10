import React, {Component} from 'react'
import {Button, Form, Input, message, Modal, Radio, Select} from 'antd'
import {FormattedMessage} from 'react-intl'
import {GLOBAL_FULL_PULL_API} from '@/app/containers/toolSet/api'
import {DATA_TABLE_SAVE_INITIAL_LOAD_CONF_API} from '@/app/containers/ResourceManage/api'
import dateFormat from 'dateformat'
// 导入样式
import styles from './res/styles/index.less'
import Request from "@/app/utils/request";
import {TABLE_GET_TABLE_ROWS_API} from "@/app/containers/ProjectManage/api";

const FormItem = Form.Item
const Option = Select.Option
const TextArea = Input.TextArea
const RadioGroup = Radio.Group

@Form.create()
export default class DataTableManageIndependentModal extends Component {
  constructor(props) {
    super(props)
    this.date = new Date()
    this.state = {
      sinkType: 'KAFKA'
    }
  }

  componentWillMount = () => {
    const {record} = this.props
    if (record && record.id) {
      Request(`${TABLE_GET_TABLE_ROWS_API}/${record.id}`, {
        method: 'get'
      })
        .then(res => {
          if (res && res.status === 0) {
            this.props.form.setFieldsValue({
              rows: res.payload
            })
          } else {
            message.warn(res.message)
          }
        })
        .catch(error => message.error(error))
    }
  }

  handleSubmit = () => {
    const {onLoading, onClose, onRequest, record} = this.props
    this.props.form.validateFieldsAndScroll((err, values) => {
      if (!err) {
        onLoading()
        const {sinkType} = this.state
        onRequest({
          api: GLOBAL_FULL_PULL_API,
          data: {
            id: this.date.getTime(),
            type: 'indepent',
            dsName: record.dsName,
            schemaName: record.schemaName,
            tableName: record.tableName,
            ctrlTopic: record.ctrlTopic,
            tableOutputTopic: record.outputTopic,
            outputTopic: values.topic,
            message: JSON.stringify({
              "from": "independent-pull-request-from-CtrlMsgSender",
              "id": this.date.getTime(),
              "payload": {
                "DBUS_DATASOURCE_ID": `${record.dsId}`,
                "INCREASE_VERSION": false,
                "INCREASE_BATCH_NO": false,
                "OP_TS": null,
                "PHYSICAL_TABLES": record.physicalTableRegex,
                "POS": null,
                "PULL_REMARK": "",
                "PULL_TARGET_COLS": "",
                "SCHEMA_NAME": record.schemaName,
                "SCN_NO": "",
                "SEQNO": this.date.getTime(),
                "SPLIT_BOUNDING_QUERY": "",
                "SPLIT_COL": "",
                "SPLIT_SHARD_SIZE": "",
                "SPLIT_SHARD_STYLE": "",
                "TABLE_NAME": record.tableName,
                "resultTopic": values.topic,
                "SINK_TYPE": sinkType,
                "HDFS_ROOT_PATH": values.hdfsRootPath
              },
              "timestamp": dateFormat(this.date, 'yyyy-mm-dd HH:MM:ss.l'),
              "type": "FULL_DATA_INDEPENDENT_PULL_REQ"
            })
          },
          method: 'post',
          callback: onClose,
        })
      }
    })
  }

  handleSave = (ope) => {
    const {record, onClose} = this.props
    this.props.form.validateFieldsAndScroll((err, values) => {
      if (!err) {
        Request(DATA_TABLE_SAVE_INITIAL_LOAD_CONF_API, {
          data: {
            ...record,
            fullpullCol: values.fullpullCol,
            fullpullSplitShardSize: values.fullpullSplitShardSize,
            fullpullSplitStyle: values.fullpullSplitStyle === undefined ? '' : values.fullpullSplitStyle,
            fullpullCondition: values.fullpullCondition
          },
          method: 'post'
        })
          .then(res => {
            if (res && res.status === 0) {
              if (ope === 'onlySave') {
                message.success(res.message)
              } else {
                this.handleSubmit()
                onClose()
              }
            } else {
              message.warn(res.message)
            }
          })
          .catch(error => message.error(error))
      }
    })
  }

  onChange = (e) => {
    this.setState({
      sinkType: e.target.value
    })
  }

  render() {
    const {getFieldDecorator} = this.props.form
    const {key, loading, visible, record, onClose} = this.props
    const {sinkType} = this.state
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
          key={key}
          visible={visible}
          maskClosable={false}
          width={1000}
          title={<FormattedMessage
            id="app.components.resourceManage.dataTable.independentFullPull"
            defaultMessage="独立拉全量"
          />}
          onCancel={onClose}
          footer={[
            <Button onClick={onClose}> 返 回 </Button>,
            <Button type="primary" onClick={() => this.handleSave('onlySave')}> 仅保存全量配置 </Button>,
            <Button type="primary" onClick={() => this.handleSave('saveAndSubmit')}> 确 定 </Button>,
          ]}
        >
          <Form>
            <FormItem label={'总数据量'} {...formItemLayout}>
              {getFieldDecorator('rows', {
                initialValue: null,
              })(<Input disabled={true} size="large" type="text"/>)}
            </FormItem>
            <FormItem label='全量类型' {...formItemLayout}>
              <RadioGroup {...formItemLayout} onChange={this.onChange} value={sinkType}>
                <Radio value='KAFKA'>KAFKA</Radio>
                <Radio value='HDFS'>HDFS</Radio>
              </RadioGroup>
            </FormItem>
            {sinkType === 'KAFKA' && (<FormItem label="Topic" {...formItemLayout}>
              {getFieldDecorator('topic', {
                initialValue: record && `independent.${record.outputTopic}.${this.date.getTime()}`,
                rules: [
                  {
                    required: true,
                    message: 'topic不能为空'
                  }
                ]
              })(<Input size="large" type="text"/>)}
            </FormItem>)}
            {sinkType === 'HDFS' && (<FormItem label="HDFS数据根目录" {...formItemLayout}>
              {getFieldDecorator('hdfsRootPath', {
                rules: [
                  {
                    required: true,
                    message: 'hdfs root path不能为空'
                  }
                ]
              })(<Input size="large" type="text" placeholder="/datahub/dbus"/>)}
            </FormItem>)}
            <FormItem label={'分片列'} {...formItemLayout}>
              {getFieldDecorator('fullpullCol', {
                initialValue: record && record.fullpullCol,
              })(<Input size="large" type="text"/>)}
            </FormItem>
            <FormItem label={'分片大小'} {...formItemLayout}>
              {getFieldDecorator('fullpullSplitShardSize', {
                initialValue: record && record.fullpullSplitShardSize,
              })(<Input size="large" type="text"/>)}
            </FormItem>
            <FormItem label={'分片类型'} {...formItemLayout}>
              {getFieldDecorator('fullpullSplitStyle', {
                initialValue: record && record.fullpullSplitStyle,
              })(
                <Select
                  showSearch
                  optionFilterProp="children"
                  allowClear={true}
                >
                  <Option value="number">number</Option>
                  <Option value="md5">md5</Option>
                  <Option value="md5big">md5big</Option>
                  <Option value="uuid">uuid</Option>
                  <Option value="uuidbig">uuidbig</Option>
                </Select>
              )}
            </FormItem>
            <FormItem label={'分片条件'} {...formItemLayout}>
              {getFieldDecorator('fullpullCondition', {
                initialValue: record && record.fullpullCondition,
              })(<Input size="large" type="text"/>)}
            </FormItem>
          </Form>
        </Modal>
      </div>
    )
  }
}

DataTableManageIndependentModal.propTypes = {}
