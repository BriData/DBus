import React, {Component} from 'react'
import {Button, Col, Form, Input, message, Row, Select} from 'antd'
import {FormattedMessage} from 'react-intl'
import dateFormat from 'dateformat'
// 导入样式
import styles from './res/styles/index.less'
import Request from '@/app/utils/request'
import {SEND_CONTROL_MESSAGE_API} from '@/app/containers/toolSet/api/index.js'

const FormItem = Form.Item
const Option = Select.Option

@Form.create()
export default class DataTableManageSearch extends Component {
  constructor(props) {
    super(props)
    this.state = {
      selectDatasource: false
    }
  }

  componentWillMount() {
  }

  handleSearch = () => {
    const {onSearch, params} = this.props
    this.props.form.validateFields((err, values) => {
      if (!err) {
        const {schemaId} = values
        onSearch({...params, ...values, schemaId: schemaId})
      }
    })
  }

  handleReset = () => {
    this.props.form.resetFields()
    this.setState({selectDatasource: false})
  }

  handleBatchReloadExtractor = selectedRows => {
    const filterdRowsMap = {}
    selectedRows.forEach(row => {
      if (row.dsType === 'mysql') filterdRowsMap[row.dsName] = row
    })
    const dsNameList = Object.keys(filterdRowsMap)
    if (!dsNameList.length) return
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

  handleGetSchemaList = dsId => {
    const {params} = this.props
    this.props.form.setFieldsValue({
      schemaId: null
    })
    if (dsId === null) {
      this.setState({selectDatasource: false})
    } else {
      this.setState({selectDatasource: true})
    }
    const {onGetSchemaList} = this.props
    onGetSchemaList(dsId)
  }

  render() {
    const {getFieldDecorator} = this.props.form
    const {dataSourceIdTypeName, schemaList, onMoveTables} = this.props
    const {onAllStart, onAllStop, onBatchFullPull, onAllDelete} = this.props
    const {selectDatasource} = this.state

    const dataSource = [{
      dsId: null, dsTypeName: <FormattedMessage
        id="app.components.resourceManage.dataSchema.selectDatasource"
        defaultMessage="请选择数据源"
      />
    }, ...Object.values(dataSourceIdTypeName.result)]

    return (
      <div className="form-search">
        <Form autoComplete="off" layout="inline" className={styles.searchForm}
              onKeyUp={e => e.keyCode === 13 && this.handleSearch()}>
          <Row>
            <Col span={10} className={styles.formLeft}>
              {/*<FormItem>*/}
              {/*<Button*/}
              {/*type="primary"*/}
              {/*icon="car"*/}
              {/*onClick={onMoveTables}*/}
              {/*>*/}
              {/*<FormattedMessage*/}
              {/*id="app.components.resourceManage.dataTable.batchMoveTopoTables"*/}
              {/*defaultMessage="批量迁移"*/}
              {/*/>*/}
              {/*</Button>*/}
              {/*</FormItem>*/}
              <FormItem>
                <Button
                  type="primary"
                  icon="export"
                  size="large"
                  onClick={onBatchFullPull}
                >
                  <FormattedMessage
                    id="app.components.resourceManage.dataTable.batchFullPull"
                    defaultMessage="批量拉全量"
                  />
                </Button>
              </FormItem>
              <FormItem>
                <Button
                  type="primary"
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
                  type="primary"
                  icon="pause"
                  onClick={onAllStop}
                >
                  <FormattedMessage
                    id="app.components.resourceManage.dataTable.batchStop"
                    defaultMessage="批量停止"
                  />
                </Button>
              </FormItem>
              <FormItem>
                <Button
                  type="primary"
                  icon="delete"
                  onClick={onAllDelete}
                >
                  <FormattedMessage
                    id="app.components.resourceManage.dataTable.batchDelete"
                    defaultMessage="批量删除"
                  />
                </Button>
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
                    onChange={this.handleGetSchemaList}
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
              {(selectDatasource && <FormItem>
                {getFieldDecorator('schemaId', {
                  initialValue: null
                })(
                  <Select
                    showSearch
                    optionFilterProp='children'
                    className={styles.input}
                    placeholder="select a data source"
                  >
                    {schemaList.map(item => (
                      <Option
                        value={item.id ? `${item.id}` : null}
                        key={`${item.id ? item.id : 'id'}`}
                      >
                        {item.schemaName}
                      </Option>
                    ))}
                  </Select>
                )}
              </FormItem>)}
              {(!selectDatasource && <FormItem>
                  {getFieldDecorator('schemaName', {
                    initialValue: null
                  })(<Input className={styles.input} placeholder="data schema name"/>)}
                </FormItem>
              )}
              <FormItem>
                {getFieldDecorator('tableName', {
                  initialValue: null
                })(<Input className={styles.input} placeholder="data table name"/>)}
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
                  type="primary"
                  icon="reload"
                  onClick={this.handleReset}
                >
                  <FormattedMessage
                    id="app.components.configCenter.mgrConfig.reset"
                    defaultMessage="重置"
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

DataTableManageSearch.propTypes = {}
