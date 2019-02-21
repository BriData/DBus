import React, { PropTypes, Component } from 'react'
import {Form, message, Select, Input, Button, Row, Col, Popconfirm} from 'antd'
import { FormattedMessage } from 'react-intl'
import dateFormat from 'dateformat'
// 导入样式
import styles from './res/styles/index.less'
import Request from "@/app/utils/request";
import {SEND_CONTROL_MESSAGE_API} from '@/app/containers/toolSet/api/index.js'
import {PROJECT_TABLE_BATCH_START_API} from "@/app/containers/ProjectManage/api";
const FormItem = Form.Item
const Option = Select.Option

@Form.create()
export default class DataTableManageSearch extends Component {
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


  handleBatchReloadExtractor = selectedRows => {
    const filterdRowsMap = {}
    selectedRows.forEach(row => {
      if (row.dsType === 'mysql') filterdRowsMap[row.dsName] = row
    })
    const dsNameList = Object.keys(filterdRowsMap)
    if (!dsNameList.length) return;
    Promise.all(dsNameList.map(dsName => {
      const date = new Date()
      const json = {
        from: 'dbus-web',
        id: date.getTime(),
        payload: {
          dsName,
          dsType: 'mysql'
        },
        timestamp: dateFormat(date, 'yyyy-mm-dd HH:MM:ss.l'),
        type: 'EXTRACTOR_RELOAD_CONF'
      }
      const data = {
        topic: filterdRowsMap[dsName].ctrlTopic,
        message: JSON.stringify(json)
      }
      return new Promise((resolve, reject) => {
        Request(SEND_CONTROL_MESSAGE_API, {
          data,
          method: 'post'
        })
          .then(res => {
            if (res && res.status === 0) {
              resolve()
            } else {
              reject(res.message)
            }
          })
          .catch(error => {
            error.response && error.response.data && error.response.data.message
              ? reject(error.response.data.message)
              : reject(error.message)
          })
      })
    })).then(() => {
      message.success('批量Reload Extractor发送成功')
    }).catch(error => {
      message.error(`批量Reload Extractor发送失败，错误信息：${error}`)
    })
  }

  render () {
    const { getFieldDecorator } = this.props.form
    const { dataSourceIdTypeName } = this.props
    const { onAllStart,onAllStop,onBatchFullPull } = this.props

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
              <FormItem>
                <Popconfirm title={'该操作为异步执行,请求结果请查看全量历史,获取批量拉全量情况!'} onConfirm={onBatchFullPull}
                            okText="Yes" cancelText="No">
                  <Button
                    icon="export"
                    size="large"
                  >
                    <FormattedMessage
                      id="app.components.resourceManage.dataTable.batchFullPull"
                      defaultMessage="批量拉全量"
                    />
                  </Button>
                </Popconfirm>
              </FormItem>
              <FormItem>
                <Button
                  icon="caret-right"
                  onClick={onAllStart}
                >
                  <FormattedMessage
                    id="app.components.resourceManage.dataTable.batchStart"
                    defaultMessage="批量启动"
                  />
                </Button>
              </FormItem>
              <FormItem>
                <Button
                  icon="pause"
                  onClick={onAllStop}
                >
                  <FormattedMessage
                    id="app.components.resourceManage.dataTable.batchStop"
                    defaultMessage="批量停止"
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

DataTableManageSearch.propTypes = {
}
