import React, {PropTypes, Component} from 'react'
import {Button, message, Modal, Form, Select, Input} from 'antd'
import {FormattedMessage} from 'react-intl'

// 导入样式
import styles from './res/styles/index.less'
import Request from "@/app/utils/request";
import {PROJECT_TABLE_GET_TOPO_ALIAS_API} from "@/app/containers/ProjectManage/api";

const FormItem = Form.Item
const Option = Select.Option
const TextArea = Input.TextArea

@Form.create()
export default class ProjectTableMoveTopoTableModal extends Component {
  constructor(props) {
    super(props)
    this.state = {topolist: []}
  }

  componentWillMount = () => {
    const {tableParams} = this.props
    if (tableParams !== null) {
      const {topoId} = tableParams
      if (topoId) {
        Request(`${PROJECT_TABLE_GET_TOPO_ALIAS_API}/${topoId}`)
          .then(res => {
            if (res && res.status === 0) {
              this.setState({
                topolist: res.payload
              })
            } else {
              message.warn(res.message)
            }
          })
          .catch(error => message.error(error))
      }
    }
  }

  handleSubmit = () => {
    const {onBatchMoveTopoTables} = this.props
    const {onClose} = this.props
    this.props.form.validateFieldsAndScroll((err, values) => {
      if (!err) {
        onBatchMoveTopoTables(values)
        onClose()
      }
    })
  }

  render() {
    const {getFieldDecorator} = this.props.form
    const {key, visible, onClose} = this.props
    const {topolist} = this.state
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
            id="app.components.resourceManage.dataTable.batchMoveTopoTables"
            defaultMessage="批量迁移"
          />}
          onCancel={onClose}
          onOk={this.handleSubmit}
        >
          <Form>
            <FormItem label="topolist" {...formItemLayout}>
              {getFieldDecorator('nameId', {
                initialValue: null
              })(
                <Select
                  // onChange={this.handleDsIdChange}
                  placeholder="Select Topology"
                >
                  {topolist.map(item => (
                    <Option value={item.nameId ? item.nameId : null} key={`${item.nameId ? item.nameId : 'nameId'}`}>
                      {item.name}
                    </Option>
                  ))}
                </Select>
              )}
            </FormItem>
          </Form>
        </Modal>
      </div>
    )
  }
}

ProjectTableMoveTopoTableModal.propTypes = {}
