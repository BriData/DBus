import React, { PropTypes, Component } from 'react'
import { Form, Select, Input, Button, Row, Col } from 'antd'
// 导入样式
import styles from './res/styles/index.less'
import { FormattedMessage } from 'react-intl'
const FormItem = Form.Item
const Option = Select.Option
@Form.create({ warppedComponentRef: true })
export default class UserKeyDownloadSearch extends Component {
  constructor (props) {
    super(props)
  }
  componentWillMount () {
  }

  render () {
    const {onOpenDownload,principal} = this.props
    const { getFieldDecorator } = this.props.form
    const formItemLayout = {
      labelCol: { span: 2 },
      wrapperCol: { span: 12 }
    }
    const tailItemLayout = {
      wrapperCol: {offset: 2, span: 12 }
    }
    return (
      <div className="form-search">
        <Form autoComplete="off" className={styles.searchForm}>
          <Row>
            <Col span={16} className={styles.formLeft}>
              <FormItem label={<FormattedMessage
                          id="app.components.projectManage.projectHome.tabs.basic.principal"
                          defaultMessage="Principal"
                        />}
                        {...formItemLayout}
              >
                {getFieldDecorator('userName', {
                  initialValue: principal,
                  rules: [
                    {
                      required: true,
                      message: '用户名不能为空'
                    },
                    {
                      pattern: /\S+/,
                      message: '请输入正确用户名'
                    }
                  ]
                })(
                  <Input
                    type="text"
                  />)}
              </FormItem>
            </Col>
          </Row>
          <Row>
            <Col span={16} className={styles.formLeft}>
              <FormItem {...tailItemLayout}>
                <Button
                  type="primary"
                  onClick={onOpenDownload}
                >
                  {"下载密钥文件"}
                </Button>
              </FormItem>
            </Col>
          </Row>
        </Form>
      </div>
    )
  }
}

UserKeyDownloadSearch.propTypes = {
}
