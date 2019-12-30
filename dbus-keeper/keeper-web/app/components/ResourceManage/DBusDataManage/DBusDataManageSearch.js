import React, { PropTypes, Component } from 'react'
import { Form, Select, Input, Button, Row, Col } from 'antd'
import { FormattedMessage } from 'react-intl'
// 导入样式
import styles from './res/styles/index.less'

const FormItem = Form.Item
const Option = Select.Option

@Form.create()
export default class DBusDataManageSearch extends Component {
  constructor (props) {
    super(props)
  }
  componentWillMount () {
  }

  handleSearch = () => {

  }

  render () {
    const {onDataSourceChange} = this.props
    const {dataSourceList, dsId} = this.props
    const dataSource = [
      {dsId: null, dsTypeName: '请选择DataSource'},
      ...dataSourceList
        .filter(ds => ds.ds_type === 'mysql' || ds.ds_type === 'oracle'
          || ds.ds_type === 'db2'
        )
        .map(ds => ({dsId: ds.dsId,dsTypeName: ds.dsTypeName}))
    ]
    return (
      <div className="form-search">
        <Form autoComplete="off" layout="inline" className={styles.searchForm}>
          <Row>
            <Col span={24} className={styles.formRight}>
              <FormItem>
                <Select
                  showSearch
                  optionFilterProp='children'
                  className={styles.select}
                  placeholder="select a data source"
                  onChange={onDataSourceChange}
                  value={dsId ? `${dsId}` : null}
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
              </FormItem>
            </Col>
          </Row>
        </Form>
      </div>
    )
  }
}

DBusDataManageSearch.propTypes = {
}
