import React, { PropTypes, Component } from 'react'
import { Popconfirm, Form, Select, Input, Button, Row, Col } from 'antd'
import { FormattedMessage } from 'react-intl'
// 导入样式
import styles from './res/styles/index.less'

const FormItem = Form.Item
const Option = Select.Option

@Form.create()
export default class RuleGroupSearch extends Component {
  constructor (props) {
    super(props)
  }
  componentWillMount () {
  }

  render () {
    const {onOpenNewModal, onDiff, onUpgradeVersion} = this.props
    const {upgradeLoading} = this.props
    return (
      <div className="form-search">
        <Form autoComplete="off" layout="inline" className={styles.searchForm}>
          <Row>
            <Col span={24} className={styles.formRight}>
              <FormItem>
                <Popconfirm title={'确定升级版本？'} onConfirm={onUpgradeVersion} okText="Yes" cancelText="No">
                  <Button
                    type="primary"
                    icon="sync"
                    loading={upgradeLoading}
                  >
                    <FormattedMessage
                      id="app.components.resourceManage.ruleGroup.upgradeVersion"
                      defaultMessage="升级版本"
                    />
                  </Button>
                </Popconfirm>
              </FormItem>
              <FormItem>
                <Button
                  icon="exception"
                  onClick={onDiff}
                >
                  <FormattedMessage
                    id="app.components.resourceManage.ruleGroup.diff"
                    defaultMessage="对比"
                  />
                </Button>
              </FormItem>
              <FormItem>
                <Button
                  icon="plus-circle"
                  onClick={onOpenNewModal}
                >
                  <FormattedMessage
                    id="app.components.resourceManage.ruleGroup.addGroup"
                    defaultMessage="添加规则组"
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

RuleGroupSearch.propTypes = {
}
