import React, { PropTypes, Component } from 'react'
import {Row, Col, Modal, Form, Select, Input, Button, message,Table } from 'antd'
import { FormattedMessage } from 'react-intl'
import OperatingButton from '@/app/components/common/OperatingButton'

// 导入样式
import styles from './res/styles/index.less'
import Request from "@/app/utils/request";

const FormItem = Form.Item
const Option = Select.Option
const TextArea = Input.TextArea

@Form.create()
export default class DataTableManageReadZkModal extends Component {
  constructor (props) {
    super(props)
    this.zkList = null
  }

  componentWillUnmount = () => {
    const {onReadZk} = this.props
    onReadZk(`/DBus`)
  }

  componentWillReceiveProps = nextProps => {
    const {node, visible} = nextProps
    if (!visible) return
    const zkList = node.result.children
    if (!this.zkList && zkList) {
      this.handleRefresh(zkList[zkList.length - 1].name)
      this.zkList = zkList
    }
  }

  handleRefresh = (value) => {
    const values = this.props.form.getFieldsValue()
    value = value || values.zkNode
    const {onReadZk, record} = this.props
    onReadZk(`/DBus/FullPuller/${record.dsName}/${record.schemaName}/${record.tableName}/${value}`)
  }

  render () {
    const { getFieldDecorator } = this.props.form
    const {key, visible, onClose, node, zkData} = this.props
    const zkList = node.result.children || []
    const zkContent = (zkData.result.payload || {}).content
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
          maskClosable={true}
          width={1000}
          title={<FormattedMessage
            id="app.components.resourceManage.dataTable.readZk"
            defaultMessage="查看全量拉取状态"
          />}
          onCancel={onClose}
          footer={[<Button type="primary" onClick={onClose}>
            <FormattedMessage
              id="app.common.back"
              defaultMessage="返回"
            />
          </Button>]}
        >

          <Form>
            <FormItem
              label={"ZK Node"} {...formItemLayout}
            >
              <Row>
                <Col style={{marginTop: 3}} span={20}>
                  {getFieldDecorator('zkNode', {
                    initialValue: zkList.length ? zkList[zkList.length - 1].name : null
                  })(
                    <Select
                      showSearch
                      optionFilterProp='children'
                      className={styles.select}
                      placeholder="Select zk node"
                      onChange={this.handleRefresh}
                    >
                      {zkList.map(item => (
                        <Option value={item.name} key={item.name}>
                          {item.name}
                        </Option>
                      ))}
                    </Select>
                  )}
                </Col>
                <Col span={4}>
                  <Button style={{marginLeft:10}} onClick={() => this.handleRefresh()}>
                    <FormattedMessage
                      id="app.common.refresh"
                      defaultMessage="刷新"
                    />
                  </Button>
                </Col>
              </Row>
            </FormItem>
            <FormItem
              label={"Content"} {...formItemLayout}
            >
              <TextArea value={zkContent} autosize={{minRows:10,maxRows:20}}/>
            </FormItem>
          </Form>
        </Modal>
      </div>
    )
  }
}

DataTableManageReadZkModal.propTypes = {
}
