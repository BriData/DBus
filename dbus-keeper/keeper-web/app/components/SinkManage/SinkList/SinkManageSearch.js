/**
 * @author 戎晓伟
 * @description  Sink信息设置
 */

import React, { PropTypes, Component } from 'react'
import { Form, Select, Input, Button, Row, Col } from 'antd'
import { FormattedMessage } from 'react-intl'
// 导入样式
import styles from './res/styles/index.less'

const FormItem = Form.Item
const Option = Select.Option

@Form.create()
export default class SinkManageSearch extends Component {
  /**
   * 校验并查询
   */
  handleSearch = e => {
    const { onSearch, sinkParams } = this.props
    const { validateFields } = this.props.form
    // 校验并查询
    validateFields((err, value) => {
      if (!err) {
        onSearch({ ...sinkParams, ...value })
      }
    })
  };
  render () {
    const { getFieldDecorator } = this.props.form
    const { onShowModal } = this.props
    return (
      <div className="form-search">
        <Form autoComplete="off" layout="inline" className={styles.searchForm} onKeyUp={e => e.keyCode === 13 && this.handleSearch()}>
          <Row>
            <Col span={6} className={styles.formLeft}>
              <FormItem>
                <Button
                  type="primary"
                  icon="search"
                  onClick={() => onShowModal(true)}
                >
                  <FormattedMessage
                    id="app.components.sinkManage.addSink"
                    defaultMessage="添加Sink"
                  />
                </Button>
              </FormItem>
            </Col>
            <Col span={18} className={styles.formRight}>
              <FormItem>
                {getFieldDecorator('sinkName', {
                  initialValue: ''
                })(<Input className={styles.input} placeholder="sink name" />)}
              </FormItem>
              <FormItem>
                <Button
                  type="primary"
                  icon="search"
                  onClick={this.handleSearch}
                >
                  <FormattedMessage
                    id="app.common.search"
                    defaultMessage="查询"
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

SinkManageSearch.propTypes = {
  form: PropTypes.object,
  onSearch: PropTypes.func
}
