import React, { PropTypes, Component } from 'react'
import { Modal, Form, Select, Input, Button, message,Table } from 'antd'
import { FormattedMessage } from 'react-intl'
import OperatingButton from '@/app/components/common/OperatingButton'
import {GLOBAL_FULL_PULL_API} from '@/app/containers/toolSet/api'
import dateFormat from 'dateformat'
// 导入样式
import styles from './res/styles/index.less'
import Request from "@/app/utils/request";

const FormItem = Form.Item
const Option = Select.Option
const TextArea = Input.TextArea

@Form.create()
export default class DataTableManageIndependentModal extends Component {
  constructor (props) {
    super(props)
    this.date = new Date()
    this.state = {
    }
  }

  handleSubmit = () => {
    const {onLoading, onClose, onRequest, record} = this.props
    this.props.form.validateFieldsAndScroll((err, values) => {
      if (!err) {
        onLoading()
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
                "resultTopic": values.topic
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

  render () {
    const { getFieldDecorator } = this.props.form
    const {key, loading, visible, record, onClose} = this.props
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
          key={key}
          visible={visible}
          maskClosable={false}
          width={1000}
          title={<FormattedMessage
            id="app.components.resourceManage.dataTable.independentFullPull"
            defaultMessage="独立拉全量"
          />}
          onCancel={onClose}
          confirmLoading={loading}
          onOk={this.handleSubmit}
        >
          <Form>
            <FormItem label="Topic" {...formItemLayout}>
              {getFieldDecorator('topic', {
                initialValue: `independent.${record.outputTopic}.${this.date.getTime()}`,
                rules: [
                  {
                    required: true,
                    message: 'topic不能为空'
                  }
                ]
              })(<Input size="large" type="text" />)}
            </FormItem>
          </Form>
        </Modal>
      </div>
    )
  }
}

DataTableManageIndependentModal.propTypes = {
}
