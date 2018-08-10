/**
 * @author 戎晓伟
 * @description  Sink信息设置
 */

import React, { PropTypes, Component } from 'react'
import { Form, Select, Input, Button, Row, Col } from 'antd'
import { FormattedMessage } from 'react-intl'
import { fromJS } from 'immutable'
// 导入样式
import styles from './res/styles/index.less'

const FormItem = Form.Item
const Option = Select.Option

@Form.create()
export default class ProjectResourceSearch extends Component {
  /**
   * 校验并查询
   */
  handleSearch = e => {
    const { validateFields } = this.props.form
    const { onSearch, resourceParams } = this.props
    // 校验并查询
    validateFields((err, value) => {
      if (!err) {
        onSearch({ ...resourceParams, ...value }, true)
      }
    })
  }

  render () {
    const { getFieldDecorator } = this.props.form
    const {dataSourceList, projectList} = this.props
    const project = projectList ? [{id: null, projectDisplayName: '请选择Project'}, ...projectList] : [{id: null, projectDisplayName: '请选择Project'}]
    const dataSource = dataSourceList ? [{dsId: null, dsName: '请选择DataSource'}, ...dataSourceList.map(ds => ({dsId:ds.dsName, dsName:ds.dsName} ))] : [{dsId: null, dsName: '请选择DataSource'}]
    const {projectId} = this.props
    return (
      <div className="form-search">
        <Form autoComplete="off" layout="inline" className={styles.searchForm} onKeyUp={e => e.keyCode === 13 && this.handleSearch()}>
          <Row>
            <Col span={6} className={styles.formLeft}>
              {!projectId && (<FormItem
                label={
                  <FormattedMessage
                    id="app.common.table.project"
                    defaultMessage="项目"
                  />
                }
              >
                {getFieldDecorator('projectId', {
                  initialValue: project && project[0].id ? `${project[0].id}` : null
                })(
                  <Select
                    showSearch
                    optionFilterProp='children'
                    className={styles.select}
                    placeholder="Select Project"
                  >
                    {project.map(item => (
                      <Option value={item.id ? `${item.id}`:null} key={`${item.id ? item.id : 'id'}`}>
                        {item.projectDisplayName}
                      </Option>
                    ))}
                  </Select>
                )}
              </FormItem>)}
            </Col>
            <Col span={18} className={styles.formRight}>
              <FormItem>
                {getFieldDecorator('dsName', {
                  initialValue: dataSource && dataSource[0].dsId ? `${dataSource[0].dsId}` : null
                })(
                  <Select
                    showSearch
                    optionFilterProp='children'
                    className={styles.select}
                    placeholder="select a data source"
                  >
                    {dataSource.map(item => (
                      <Option value={item.dsId ? `${item.dsId}` : null} key={`${item.dsId ? item.dsId : 'dsId'}`}>
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

ProjectResourceSearch.propTypes = {
  form: PropTypes.object,
  local: PropTypes.any,
  resourceParams: PropTypes.object,
  onSearch: PropTypes.func
}
