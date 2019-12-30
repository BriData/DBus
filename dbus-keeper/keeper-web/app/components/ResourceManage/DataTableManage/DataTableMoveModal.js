import React, {Component} from 'react'
import {Col, Form, Modal, Row, Select} from 'antd'
import {FormattedMessage} from 'react-intl'
// 导入样式
import styles from './res/styles/index.less'

const FormItem = Form.Item
const Option = Select.Option

@Form.create()
export default class DataTableMoveModal extends Component {
  constructor(props) {
    super(props)
  }

  componentWillMount() {
  }

  handleSubmit = () => {
    const {onMoveTable} = this.props
    this.props.form.validateFieldsAndScroll((err, values) => {
      if (!err) {
        onMoveTable(values)
      }
    })
  }

  render() {
    const {getFieldDecorator} = this.props.form
    const {dataSourceIdTypeName} = this.props
    const {onClose, key, visible} = this.props

    const dataSource = [{
      dsId: null, dsTypeName: <FormattedMessage
        id="app.components.resourceManage.dataSchema.selectDatasource"
        defaultMessage="请选择数据源"
      />
    }, ...Object.values(dataSourceIdTypeName.result)]

    return (
      <div className={styles.table}>
        <Modal
          key={key}
          visible={visible}
          maskClosable={false}
          width={700}
          title={<FormattedMessage
            id="app.components.resourceManage.dataTable.batchMoveTopoTables"
            defaultMessage="批量迁移"
          />}
          onCancel={onClose}
          onOk={this.handleSubmit}
        >
          <span>请选择目标数据源</span>
          <Form autoComplete="off" layout="inline" className={styles.searchForm}>
            <Row>
              <Col span={24} className={styles.formLeft}>
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
              </Col>
            </Row>
          </Form>
        </Modal>
      </div>
    )
  }
}

DataTableMoveModal.propTypes = {}
