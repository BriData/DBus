import React, {Component} from 'react'
import {Form, Input, Modal} from 'antd'
import {FormattedMessage} from 'react-intl'
// 导入样式
import styles from './res/styles/index.less'

const FormItem = Form.Item

@Form.create()
export default class SearchDatasourceExistModal extends Component {
  constructor (props) {
    super(props)
    this.state = {}
  }

  handleSubmit = () => {
    const {onSearch} = this.props
    this.props.form.validateFieldsAndScroll((err, values) => {
      if(!err){
        onSearch(values)
      }
    })
  }

  render () {
    const {getFieldDecorator} = this.props.form
    const {key, visible, onClose} = this.props
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
            id="app.components.resourceManage.dataSource.searchDatabaseExist"
            defaultMessage="查询数据源是否接入"
          />}
          onCancel={onClose}
          onOk={this.handleSubmit}
        >
          <Form autoComplete="off" className="data-source-modify-form">
            <FormItem label={<FormattedMessage
              id="app.components.resourceManage.dataSource.ip"
              defaultMessage="IP"
            />} {...formItemLayout}>
              {getFieldDecorator('ip', {
                rules: [
                  {
                    required: true,
                    message: '不能为空'
                  }
                ]
              })(<Input size="default" type="text"/>)}
            </FormItem>
            <FormItem label={<FormattedMessage
              id="app.components.resourceManage.dataSource.port"
              defaultMessage="端口号"
            />} {...formItemLayout}>
              {getFieldDecorator('port', {
                rules: [
                  {
                    required: true,
                    message: '不能为空'
                  }
                ]
              })(<Input size="default" type="text"/>)}
            </FormItem>
          </Form>
        </Modal>
      </div>
    )
  }
}

SearchDatasourceExistModal.propTypes = {}
