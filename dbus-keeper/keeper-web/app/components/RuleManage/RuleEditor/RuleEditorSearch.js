import React, { PropTypes, Component } from 'react'
import { Popconfirm, Form, Select, Input, Button, Row, Col } from 'antd'
import { FormattedMessage } from 'react-intl'
// 导入样式
import styles from './res/styles/index.less'

const FormItem = Form.Item
const Option = Select.Option

@Form.create()
export default class RuleEditorSearch extends Component {
  constructor (props) {
    super(props)
  }

  componentWillMount () {
  }

  handleShowData = () => {
    const {onExecuteRule} = this.props
    onExecuteRule([])
  }

  handleBack = () => {
    window.history.back()
  }

  render () {
    const { getFieldDecorator } = this.props.form
    const {kafkaOffset, kafkaCount, kafkaTopic} = this.props
    const {onSaveAllRules, onAddRule} = this.props
    return (
      <div className="form-search">
        <Form autoComplete="off" layout="inline" className={styles.searchForm}>
          <Row>
            <Col span={15}>
              <FormItem label='Topic'>
                {getFieldDecorator('kafkaTopic', {
                  initialValue: kafkaTopic,
                  rules: [
                    {
                      required: true,
                      message: 'Topic不能为空'
                    }
                  ]
                })(
                  <Input
                    placeholder="请输入topic"
                    type="text"
                  />
                )}
              </FormItem>
              <FormItem label='Offset'>
                {getFieldDecorator('kafkaOffset', {
                  initialValue: kafkaOffset,
                  rules: [
                    {
                      required: true,
                      message: 'offset不能为空'
                    },
                    {
                      pattern: /\d+/,
                      message: '请输入正确数字'
                    }
                  ]
                })(
                  <Input
                    placeholder="请输入offset"
                    type="text"
                    style={{width: 80}}
                  />
                )}
              </FormItem>
              <FormItem label={<FormattedMessage
                id="app.common.count"
                defaultMessage="数量"
              />}>
                {getFieldDecorator('kafkaCount', {
                  initialValue: kafkaCount,
                  rules: [
                    {
                      required: true,
                      message: 'Count不能为空'
                    },
                    {
                      pattern: /\d+/,
                      message: '请输入正确数字'
                    }
                  ]
                })(
                  <Input
                    placeholder="请输入Count"
                    type="text"
                    style={{width: 80}}
                  />
                )}
              </FormItem>
              <FormItem>
                <Button
                  icon="file-text"
                  onClick={this.handleShowData}
                >
                  <FormattedMessage
                    id="app.components.resourceManage.rule.showData"
                    defaultMessage="展示数据"
                  />
                </Button>
              </FormItem>
            </Col>
            <Col span={9} className={styles.formRight}>
              <FormItem>
                <Button
                  icon="plus-circle"
                  onClick={onAddRule}
                >
                  <FormattedMessage
                    id="app.components.resourceManage.rule.addRule"
                    defaultMessage="添加规则"
                  />
                </Button>
              </FormItem>
              <FormItem>
                  <Button
                    type="primary"
                    icon="save"
                    onClick={onSaveAllRules}
                  >
                    <FormattedMessage
                      id="app.components.resourceManage.rule.saveAllRules"
                      defaultMessage="保存所有规则"
                    />
                  </Button>
              </FormItem>
              <FormItem>
                <Button
                  icon="rollback"
                  onClick={this.handleBack}
                >
                  <FormattedMessage
                    id="app.common.back"
                    defaultMessage="返回"
                  />
                </Button>
              </FormItem>
            </Col>
          </Row>
        </Form>
      </div>
    )
  }
}

RuleEditorSearch.propTypes = {
}
