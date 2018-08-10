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
export default class EncodeManagerSearch extends Component {
  /**
   * 校验并查询
   */
  handleSearch = e => {
    const { validateFields } = this.props.form
    const { onSearch, params } = this.props
    // 校验并查询
    validateFields((err, value) => {
      if (!err) {
        onSearch({ ...params, ...value }, true)
      }
    })
  };

  render () {
    const { getFieldDecorator } = this.props.form
    const {
      dataSourceList,
      projectList,
      topologyList,
      onProjectSelect
    } = this.props
    const project = [
      { projectId: null, projectDisplayName: '请选择Project' },
      ...Object.values(projectList.result)
    ] || [{ projectId: null, projectDisplayName: '请选择Project' }]
    const dataSource = [
      { dsId: null, dsName: '请选择DataSource' },
      ...Object.values(dataSourceList.result).map(ds => ({dsId: ds.dsId, dsName: ds.ds_name}))
    ] || [{ dsId: null, dsName: '请选择DataSource' }]
    const topology = [
      { topoId: null, topoName: '请选择Topology' },
      ...Object.values(topologyList.result)
    ] || [{ topoId: null, topoName: '请选择Topology' }]
    return (
      <div className="form-search">
        <Form autoComplete="off" layout="inline" className={styles.searchForm} onKeyUp={e => e.keyCode === 13 && this.handleSearch()}>
          <Row>
            <Col span={10}>
                <FormItem
                  label={
                    <FormattedMessage
                      id="app.common.table.project"
                      defaultMessage="项目"
                    />
                  }
                >
                  {getFieldDecorator('projectId', {
                    initialValue: null
                  })(
                    <Select
                      showSearch
                      optionFilterProp='children'
                      className={styles.select}
                      placeholder="Select Project"
                      onSelect={value => {
                        this.props.form.setFieldsValue({topoId:null})
                        onProjectSelect(value)
                      }}
                    >
                      {project.map(item => (
                        <Option
                          value={item.projectId ? `${item.projectId}` : null}
                          key={`${
                            item.projectId ? item.projectId : 'projectId'
                          }`}
                        >
                          {item.projectDisplayName}
                        </Option>
                      ))}
                    </Select>
                  )}
                </FormItem>


                <FormItem label={
                  <FormattedMessage id="app.common.topo" defaultMessage="拓扑" />
                }>
                  {getFieldDecorator('topoId', {
                    initialValue: null
                  })(
                    <Select
                      showSearch
                      optionFilterProp='children'
                      className={styles.select}
                      placeholder="Select Topology"
                    >
                      {topology.map(item => (
                        <Option
                          value={item.topoId ? `${item.topoId}` : null}
                          key={`${item.topoId ? item.topoId : 'topoId'}`}
                        >
                          {item.topoName}
                        </Option>
                      ))}
                    </Select>
                  )}
                </FormItem>

            </Col>
            <Col span={14} className={styles.formRight}>
              <FormItem>
                {getFieldDecorator('dsId', {
                  initialValue: null
                })(
                  <Select
                    showSearch
                    optionFilterProp='children'
                    className={styles.select}
                    placeholder="select a data source"
                  >
                    {dataSource.map(item => (
                      <Option
                        value={item.dsId ? `${item.dsId}` : null}
                        key={`${item.dsId ? item.dsId : 'dsId'}`}
                      >
                        {item.dsName}
                      </Option>
                    ))}
                  </Select>
                )}
              </FormItem>
              <FormItem>
                {getFieldDecorator('schemaName', {
                  initialValue: null
                })(<Input className={styles.input} placeholder="schemaName" />)}
              </FormItem>
              <FormItem>
                {getFieldDecorator('tableName', {
                  initialValue: null
                })(<Input className={styles.input} placeholder="tableName" />)}
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

EncodeManagerSearch.propTypes = {
}
