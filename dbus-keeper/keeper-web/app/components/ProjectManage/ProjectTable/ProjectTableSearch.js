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
export default class ProjectTableSearch extends Component {
  /**
   * 校验并查询
   */
  handleSearch = e => {
    const { validateFields } = this.props.form
    const { onSearch, tableParams } = this.props
    // 校验并查询
    validateFields((err, value) => {
      if (!err) {
        onSearch({ ...tableParams, ...value }, true)
      }
    })
  };
  /**
   * @param id [object String] 项目ID
   * @description 获取TopoList
   */
  handleChangeTopo = id => {
    const { onSetProjectId } = this.props
    onSetProjectId(id)
    const { onGetTopologyList } = this.props
    onGetTopologyList({ projectId: id })
    this.props.form.setFieldsValue({topoId:null})
  };
  render () {
    const { projectId } = this.props
    const { getFieldDecorator } = this.props.form
    const {
      dataSourceList,
      projectList,
      topologyList,
      onCreateTable,
      isCreate
    } = this.props
    const project = [
      { projectId: null, projectDisplayName: '请选择Project' },
      ...Object.values(projectList.result)
    ] || [{ projectId: null, projectDisplayName: '请选择Project' }]
    const dataSource = [
      { dsId: null, dsName: '请选择DataSource' },
      ...Object.values(dataSourceList.result)
    ] || [{ dsId: null, dsName: '请选择DataSource' }]
    const topology = [
      { topoId: null, topoName: '请选择Topology' },
      ...Object.values(topologyList.result)
    ] || [{ topoId: null, topoName: '请选择Topology' }]
    return (
      <div className="form-search">
        <Form autoComplete="off" layout="inline" className={styles.searchForm} onKeyUp={e => e.keyCode === 13 && this.handleSearch()}>
          <Row>
            <Col span={isCreate ? 2 : 11} className={styles.formLeft}>
              {(
                <Button
                  size="large"
                  type="primary"
                  icon="plus"
                  onClick={() => onCreateTable(true)}
                  className={styles.button}
                >
                  <FormattedMessage
                    id="app.common.added"
                    defaultMessage="新增"
                  />
                </Button>
              )}
              {!isCreate && (
                <FormItem
                  label={
                    <FormattedMessage
                      id="app.common.table.project"
                      defaultMessage="项目"
                    />
                  }
                >
                  {getFieldDecorator('projectId', {
                    initialValue:
                      project && project[0].projectId
                        ? `${project[0].projectId}`
                        : null
                  })(
                    <Select
                      showSearch
                      optionFilterProp='children'
                      className={styles.select}
                      placeholder="Select Project"
                      onChange={value => this.handleChangeTopo(value)}
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
              )}
              {!isCreate && (
                <FormItem label="Topology">
                  {getFieldDecorator('topoId', {
                    initialValue:
                      topology && topology[0].topoId
                        ? `${topology[0].topoId}`
                        : null
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
              )}
            </Col>
            <Col span={isCreate ? 22 : 13} className={styles.formRight}>
              {!!isCreate && (
              <FormItem label="Topology">
                {getFieldDecorator('topoId', {
                  initialValue:
                    topology && topology[0].topoId
                      ? `${topology[0].topoId}`
                      : null
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
              )}
              <FormItem>
                {getFieldDecorator('dsName', {
                  initialValue:
                    dataSource && dataSource[0].dsName === '请选择DataSource'
                      ? null
                      : `${dataSource[0].dsName}`
                })(
                  <Select
                    showSearch
                    optionFilterProp='children'
                    className={styles.select}
                    placeholder="select a data source"
                  >
                    {dataSource.map(item => (
                      <Option
                        value={
                          item.dsName === '请选择DataSource'
                            ? null
                            : `${item.dsName}`
                        }
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

ProjectTableSearch.propTypes = {
  form: PropTypes.object,
  locale: PropTypes.any,
  tableParams: PropTypes.object,
  dataSourceList: PropTypes.object,
  projectList: PropTypes.object,
  topologyList: PropTypes.object,
  onSearch: PropTypes.func,
  onGetTopologyList: PropTypes.func,
  onCreateTable: PropTypes.func
}
