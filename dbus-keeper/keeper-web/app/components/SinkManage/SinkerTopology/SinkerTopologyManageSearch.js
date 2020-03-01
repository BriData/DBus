import React, {Component, PropTypes} from 'react'
import {Button, Col, Form, Input, Row, Select} from 'antd'
import {FormattedMessage} from 'react-intl'
// 导入样式
import styles from './res/styles/index.less'

const FormItem = Form.Item
const Option = Select.Option

@Form.create()
export default class SinkerTopologyManageSearch extends Component {
  /**
   * 校验并查询
   */
  handleSearch = e => {
    const {onSearch, sinkerParams} = this.props
    const {validateFields} = this.props.form
    // 校验并查询
    validateFields((err, value) => {
      if (!err) {
        onSearch({...sinkerParams, ...value})
      }
    })
  }

  render() {
    const {getFieldDecorator} = this.props.form
    const {onOpen} = this.props
    return (
      <div className="form-search">
        <Form autoComplete="off" layout="inline" className={styles.searchForm}
              onKeyUp={e => e.keyCode === 13 && this.handleSearch()}>
          <Row>
            <Col span={6} className={styles.formLeft}>
              <FormItem>
                <Button
                  type="primary"
                  icon="plus"
                  onClick={onOpen}
                >
                  <FormattedMessage
                    id="app.components.sinkManage.sinkerTopo.addSinkerTopology"
                    defaultMessage="添加Sinker"
                  />
                </Button>
              </FormItem>
            </Col>
            <Col span={18} className={styles.formRight}>
              <FormItem>
                {getFieldDecorator('sinkerName', {
                  initialValue: ''
                })(<Input className={styles.input} placeholder="Sinker名称"/>)}
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

SinkerTopologyManageSearch.propTypes = {
  form: PropTypes.object,
  onSearch: PropTypes.func
}
