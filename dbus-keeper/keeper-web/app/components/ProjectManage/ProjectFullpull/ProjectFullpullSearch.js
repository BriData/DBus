/**
 * @author Hongchun Yin
 * @description  全量拉取历史列表
 */

import React, {Component, PropTypes} from 'react'
import {Button, Col, Form, Input, Row, Select} from 'antd'
import {FormattedMessage} from 'react-intl'
// 导入样式
import styles from './res/styles/index.less'

const FormItem = Form.Item
const Option = Select.Option

@Form.create()
export default class ProjectFullpullSearch extends Component {
  constructor(props) {
    super(props)
    this.orderColumns = [
      {value: 'id', text: 'ID'},
      {value: 'fullPullReqMsgOffset', text: '拉取信息offset'},
      {value: 'initTime', text: '初始化时间'},
      {value: 'startSplitTime', text: '开始分片时间'},
      {value: 'startPullTime', text: '开始拉取时间'},
      {value: 'endTime', text: '完成时间'},
      {value: 'updateTime', text: '更新时间'}
    ]
  }

  /**
   * 校验并查询
   */
  handleSearch = e => {
    const {validateFields} = this.props.form
    const {onSearch, fullpullParams} = this.props
    // 校验并查询
    validateFields((err, value) => {
      if (!err) {
        if (value.projectName === '请选择Project') {
          value.projectName = null
        }
        if (value.dsName === '请选择DataSource') {
          value.dsName = null
        }
        if (value.state === '请选择状态') {
          value.state = null
        }
        onSearch({...fullpullParams, ...value}, true)
      }
    })
  }

  render() {
    const {getFieldDecorator} = this.props.form
    const {query} = this.props
    const {dataSourceList, projectList} = this.props
    const project = projectList ? [{projectName: '请选择Project'}, ...projectList] : [{projectName: '请选择Project'}]
    const dataSource = dataSourceList ? [{dsName: '请选择DataSource'}, ...dataSourceList.map(ds => ({dsName: ds.dsName}))] : [{dsName: '请选择DataSource'}]

    return (
      <div className="form-search">
        <Form autoComplete="off" layout="inline" className={styles.searchForm}
              onKeyUp={e => e.keyCode === 13 && this.handleSearch()}>
          <Row>
            <Col span={24} className={styles.formRight}>
              <FormItem>
                {getFieldDecorator('projectName', {
                  initialValue: query.projectName
                })(
                  <Select
                    showSearch
                    optionFilterProp='children'
                    className={styles.select}
                    placeholder="选择项目"
                  >
                    {project.map(item => (
                      <Option value={`${item.projectName}`}
                              key={`${item.projectName ? item.projectName : 'projectName'}`}>
                        {item.projectName}
                      </Option>
                    ))}
                  </Select>
                )}
              </FormItem>
              <FormItem>
                {getFieldDecorator('orderBy', {
                  rules: [
                    {
                      message: '请选择消息类型'
                    }
                  ]
                })(
                  <Select
                    showSearch
                    optionFilterProp='children'
                    className={styles.select}
                    placeholder="选择排序列"
                  >
                    {this.orderColumns.map(item => (
                      <Option value={item.value} key={item.value}>
                        {item.text}
                      </Option>
                    ))}
                  </Select>
                )}
              </FormItem>
              <FormItem>
                {getFieldDecorator('state', {
                  rules: [
                    {
                      message: '请选择状态类型'
                    }
                  ]
                })(
                  <Select
                    showSearch
                    optionFilterProp='children'
                    className={styles.select}
                    placeholder="选择状态"
                  >
                    <Option value='请选择状态'>请选择状态</Option>
                    <Option value='init'>init</Option>
                    <Option value='splitting'>splitting</Option>
                    <Option value='pulling'>pulling</Option>
                    <Option value='ending'>ending</Option>
                    <Option value='abort'>abort</Option>
                  </Select>
                )}
              </FormItem>
              <FormItem>
                {getFieldDecorator('dsName', {
                  initialValue: query.dsName
                })(
                  <Select
                    showSearch
                    optionFilterProp='children'
                    className={styles.select}
                    placeholder="选择数据源"
                  >
                    {dataSource.map(item => (
                      <Option value={`${item.dsName}`} key={`${item.dsName ? item.dsName : 'dsName'}`}>
                        {item.dsName}
                      </Option>
                    ))}
                  </Select>
                )}
              </FormItem>
              <FormItem>
                {getFieldDecorator('schemaName', {
                  initialValue: query.schemaName
                })(<Input className={styles.input} placeholder="输入Schema"/>)}
              </FormItem>
              <FormItem>
                {getFieldDecorator('tableName', {
                  initialValue: query.tableName
                })(<Input className={styles.input} placeholder="输入表名"/>)}
              </FormItem>
              <FormItem>
                {getFieldDecorator('id', {
                  initialValue: query.id
                })(<Input className={styles.input} placeholder="输入ID"/>)}
              </FormItem>
              <FormItem>
                {getFieldDecorator('targetSinkTopic', {
                  initialValue: query.targetSinkTopic
                })(<Input className={styles.input} placeholder="输入目标Topic"/>)}
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

ProjectFullpullSearch.propTypes = {
  form: PropTypes.object,
  local: PropTypes.any,
  fullpullParams: PropTypes.object,
  onSearch: PropTypes.func
}
