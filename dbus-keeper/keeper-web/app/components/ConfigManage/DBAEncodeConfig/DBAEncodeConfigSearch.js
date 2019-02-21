import React, { PropTypes, Component } from 'react'
import { Form,message, Select, Input, Button, Row, Col } from 'antd'
import { FormattedMessage } from 'react-intl'
// 导入样式
import styles from './res/styles/index.less'
import Request from "@/app/utils/request";
const FormItem = Form.Item
const Option = Select.Option

@Form.create()
export default class DBAEncodeConfigSearch extends Component {
  constructor (props) {
    super(props)
  }
  componentWillMount () {
  }

  handleSearch = () => {
    const {onSearch, dbaEncodeParams} = this.props
    this.props.form.validateFields((err, values) => {
      if (!err) {
        onSearch({...dbaEncodeParams, ...values})
      }
    })
  }

  render () {
    const { getFieldDecorator } = this.props.form
    const {dataSourceIdTypeName} = this.props
    const dataSource = [{dsId: null, dsTypeName: <FormattedMessage
        id="app.components.resourceManage.dataSchema.selectDatasource"
        defaultMessage="请选择数据源"
      />}, ...Object.values(dataSourceIdTypeName.result)]
    return (
      <div className="form-search">
        <Form autoComplete="off" layout="inline" className={styles.searchForm} onKeyUp={e => e.keyCode === 13 && this.handleSearch()}>
          <Row>
            <Col span={24} className={styles.formRight}>
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
                        {item.dsTypeName}
                      </Option>
                    ))}
                  </Select>
                )}
              </FormItem>
              <FormItem>
                {getFieldDecorator('schemaName', {
                  initialValue: null
                })(<Input className={styles.input} placeholder="data schema name" />)}
              </FormItem>
              <FormItem>
                {getFieldDecorator('tableName', {
                  initialValue: null
                })(<Input className={styles.input} placeholder="data table name" />)}
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

DBAEncodeConfigSearch.propTypes = {
}
