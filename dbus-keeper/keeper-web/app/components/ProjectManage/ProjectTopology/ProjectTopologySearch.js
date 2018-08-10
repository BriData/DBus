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
export default class ProjectTopologySearch extends Component {
  /**
   * 校验并查询
   */
  handleSearch = e => {
    const { validateFields } = this.props.form
    const { onSearch, topologyParams } = this.props
    // 校验并查询
    validateFields((err, value) => {
      if (!err) {
        onSearch({ ...topologyParams, ...value }, true)
      }
    })
  };
  render () {
    const { onCreateTopology, onProjectIdChange, projectList, topologyList, projectId, isCreate } = this.props
    const { getFieldDecorator } = this.props.form
    const isCanCreateTopology =
      topologyList.result.isCanCreateTopology === undefined
        ? 1
        : topologyList.result.isCanCreateTopology
    const { result } = projectList
    const project = [
      { id: null, project_display_name: '请选择Project' },
      ...Object.values(result)
    ] || [{ id: null, project_display_name: '请选择Project' }]
    return (
      <div className="form-search">
        <Form autoComplete="off" layout="inline" className={styles.searchForm} onKeyUp={e => e.keyCode === 13 && this.handleSearch()}>
          <Row>
            <Col span={12} className={styles.formLeft}>
              {
                <Button
                  size="large"
                  type="primary"
                  icon="plus"
                  onClick={() => onCreateTopology(true)}
                  disabled={!isCanCreateTopology}
                  className={styles.button}
              >
                  <FormattedMessage id="app.common.added" defaultMessage="新增" />
                </Button>
              }
              {
                !isCreate && <FormItem
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
                      onChange={onProjectIdChange}
                  >
                      {project.map(item => (
                        <Option
                          value={item.id ? `${item.id}` : null}
                          key={item.id ? `${item.id}` : '0000'}
                      >
                          {item.project_display_name}
                        </Option>
                    ))}
                    </Select>
                )}
                </FormItem>
              }
            </Col>
            <Col span={12} className={styles.formRight}>
              <FormItem>
                {getFieldDecorator('topoName', {
                  initialValue: ''
                })(<Input className={styles.input} placeholder="topoName" />)}
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

ProjectTopologySearch.propTypes = {
  form: PropTypes.object,
  local: PropTypes.any,
  projectList: PropTypes.object,
  topologyList: PropTypes.object,
  topologyParams: PropTypes.object,
  onSearch: PropTypes.func,
  onCreateTopology: PropTypes.func
}
