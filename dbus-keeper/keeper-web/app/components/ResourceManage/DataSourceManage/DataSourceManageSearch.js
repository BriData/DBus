import React, { PropTypes, Component } from 'react'
import { Form, Select, Input, Button, Row, Col } from 'antd'
import { FormattedMessage } from 'react-intl'
// 导入样式
import styles from './res/styles/index.less'

const FormItem = Form.Item
const Option = Select.Option

@Form.create()
export default class DataSourceManageSearch extends Component {
  constructor (props) {
    super(props)
  }
  componentWillMount () {
  }

  handleSearch = () => {
    const {onSearch, params} = this.props
    this.props.form.validateFields((err, values) => {
      if (!err) {
        onSearch({...params, ...values})
      }
    })
  }

  render () {
    const { getFieldDecorator } = this.props.form
    const {onCreateDataSource, onBatchAddTable, onPreProcess, onGenerateOggTrailName} = this.props
    const dsTypeList = [
      null,
      'mysql',
      'oracle',
      'mongo',
      'log_logstash',
      'log_logstash_json',
      'log_ums',
      'log_flume',
      'log_filebeat',
      'jsonlog',
    ]
    return (
      <div className="form-search">
        <Form autoComplete="off"
          layout="inline"
          className={styles.searchForm}
          onKeyUp={e => e.keyCode === 13 && this.handleSearch()}
        >
          <Row>
            <Col span={12} className={styles.formLeft}>
              <FormItem>
                <Button
                  onClick={onCreateDataSource}
                  type="primary"
                >
                  <FormattedMessage
                    id="app.components.resourceManage.dataSource.newDataLine"
                    defaultMessage="新建数据线"
                  />
                </Button>
              </FormItem>
              <FormItem>
                <Button
                  onClick={onGenerateOggTrailName}
                  type="primary"
                >
                  生成OGG Trail前缀
                </Button>
              </FormItem>
            </Col>
            <Col span={12} className={styles.formRight}>
              <FormItem>
                {getFieldDecorator('dsType', {
                  initialValue: null
                })(
                  <Select
                    showSearch
                    optionFilterProp='children'
                    className={styles.select}
                    placeholder="select a data source"
                  >
                    {dsTypeList.map(dsType => (
                      <Option
                        value={dsType}
                        key={dsType}
                      >
                        {dsType ? dsType : <FormattedMessage
                          id="app.components.resourceManage.dataSource.selectDatasourceType"
                          defaultMessage="请选择数据源类型"
                        />}
                      </Option>
                    ))}
                  </Select>
                )}
              </FormItem>
              <FormItem>
                {getFieldDecorator('dsName', {
                  initialValue: ''
                })(<Input className={styles.input} placeholder="data source name" />)}
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

DataSourceManageSearch.propTypes = {
}
