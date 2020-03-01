/**
 * @author 戎晓伟
 * @description  Sink信息设置
 */

import React, {Component, PropTypes} from 'react'
import {Button, Col, Form, Input, Popconfirm, Row, Select} from 'antd'
import {FormattedMessage} from 'react-intl'
// 导入样式
import styles from './res/styles/index.less'

const FormItem = Form.Item
const Option = Select.Option

@Form.create()
export default class SinkerSchemaSearch extends Component {
  /**
   * 校验并查询
   */
  handleSearch = e => {
    const {onSearch, searchParams} = this.props
    const {validateFields} = this.props.form
    // 校验并查询
    validateFields((err, value) => {
      if (!err) {
        onSearch({...searchParams, ...value})
      }
    })
  };

  handleReset = () => {
    this.props.form.resetFields()
    this.setState({selectDatasource: false})
  }

  render() {
    const {getFieldDecorator} = this.props.form
    const {onBatchAddTable, onBatchDeleteSchema} = this.props
    return (
      <div className="form-search">
        <Form autoComplete="off" layout="inline" className={styles.searchForm}
              onKeyUp={e => e.keyCode === 13 && this.handleSearch()}>
          <Row>
            <Col span={8} className={styles.formLeft}>
              <FormItem>
                <Popconfirm title='批量加表' onConfirm={() => onBatchAddTable()} okText="Yes"
                            cancelText="No">
                  <Button
                    type="primary"
                    icon="plus"
                  >
                    批量加表
                  </Button>
                </Popconfirm>
              </FormItem>
              <FormItem>
                <Popconfirm title='批量删除Schema' onConfirm={() => onBatchDeleteSchema()} okText="Yes"
                            cancelText="No">
                  <Button
                    type="primary"
                    icon="delete"
                  >
                    批量删除Schema
                  </Button>
                </Popconfirm>
              </FormItem>
            </Col>
            <Col span={16} className={styles.formRight}>
              <FormItem>
                {getFieldDecorator('sinkerName', {
                  initialValue: ''
                })(<Input className={styles.input} placeholder="Sinker名称"/>)}
              </FormItem>
              <FormItem>
                {getFieldDecorator('dsName', {
                  initialValue: ''
                })(<Input className={styles.input} placeholder="数据源名称"/>)}
              </FormItem>
              <FormItem>
                {getFieldDecorator('schemaName', {
                  initialValue: ''
                })(<Input className={styles.input} placeholder="Schema名称"/>)}
              </FormItem>
              <FormItem>
                {getFieldDecorator('targetTopic', {
                  initialValue: ''
                })(<Input className={styles.input} placeholder="源Topic名称"/>)}
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
              <FormItem>
                <Button
                  type="primary"
                  icon="reload"
                  onClick={this.handleReset}
                >
                  <FormattedMessage
                    id="app.components.configCenter.mgrConfig.reset"
                    defaultMessage="重置"
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

SinkerSchemaSearch.propTypes = {
  form: PropTypes.object,
  onSearch: PropTypes.func
}
