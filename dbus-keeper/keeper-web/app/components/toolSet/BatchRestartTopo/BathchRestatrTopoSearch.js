import React, {PropTypes, Component} from 'react'
import {Form, Select, Input, Button, Row, Col, message} from 'antd'
import {FormattedMessage} from 'react-intl'
// 导入样式
import styles from './res/styles/index.less'
import Request from "@/app/utils/request";


const FormItem = Form.Item
const Option = Select.Option

@Form.create()
export default class BathchRestatrTopoSearch extends Component {
  constructor(props) {
    super(props)
  }

  componentWillMount() {
  }

  handleSearch = () => {
    const {onSearch, params} = this.props
    this.props.form.validateFields((err, values) => {
      if (!err) {
        onSearch({...params, ...values})
      }
    })
  }

  render() {
    const {getFieldDecorator} = this.props.form
    const {onOpenRestart} = this.props
    // const {} = this.props
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
            <Col span={24} className={styles.formRight}>
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
                })(<Input className={styles.input} placeholder="data source name"/>)}
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
                  icon="reload"
                  onClick={() => onOpenRestart()}
                >
                  <FormattedMessage
                    id="app.components.toolset.BatchRestartTopo.restart"
                    defaultMessage="批量重启拓扑"
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

BathchRestatrTopoSearch.propTypes = {}
