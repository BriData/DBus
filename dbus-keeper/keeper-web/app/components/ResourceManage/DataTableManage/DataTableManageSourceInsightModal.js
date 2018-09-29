import React, { PropTypes, Component } from 'react'
import { Button, Modal, Form, Select, Input, Spin,Table, Row, Col } from 'antd'
import { FormattedMessage } from 'react-intl'
import OperatingButton from '@/app/components/common/OperatingButton'

// 导入样式
import styles from './res/styles/index.less'
import Request from "@/app/utils/request";

const FormItem = Form.Item
const Option = Select.Option
const TextArea = Input.TextArea

@Form.create()
export default class DataTableManageSourceInsightModal extends Component {
  constructor (props) {
    super(props)
  }

  handleChange = value => {
    const {getSourceInsight, tableInfo} = this.props
    getSourceInsight({
      tableId: tableInfo.id,
      number: value
    })
  }
  renderComponent = render => (text, record, index) =>
    render(text, record, index);

  renderNo = (text, record, index) => (
    <div
      title={text}
      style={{width: 30}}
      className={styles.ellipsis}
    >
      {text}
    </div>
  );

  renderNomal = (text, record, index) => (
    <div
      title={text}
      className={styles.ellipsis}
    >
      {text !== null || text !== undefined ? text : <span style={{color: '#cacaca'}}>null</span>}
    </div>
  );

  render () {
    const { getFieldDecorator } = this.props.form
    const {key, visible, onClose, sourceInsightResult} = this.props
    const numberList = [10, 20, 30, 40, 50]
    const loading = sourceInsightResult.loading
    const sourceInsight = sourceInsightResult.result.payload || {}
    const columnInfos = sourceInsight.columns || []
    const splitColumn = sourceInsight.splitColumn
    const dataSource = columnInfos
    const columnSet = new Set()
    columnInfos.forEach(column => {
      Object.keys(column).forEach(key => columnSet.add(key))
    })
    const columns = [
      ...Array.from(columnSet).map(column => ({
        title: column,
        dataIndex: column,
        key: column,
        // width: 150,
        render: this.renderComponent(this.renderNomal)
      }))
    ]
    return (
      <div className={styles.table}>
        <Modal
          className="top-modal"
          key={key}
          visible={visible}
          maskClosable={true}
          width={1000}
          title={<FormattedMessage
            id="app.components.resourceManage.dataTable.viewSourceColumn"
            defaultMessage="查看源端表列信息"
          />}
          onCancel={onClose}
          footer={[<Button type="primary" onClick={onClose}> 返 回 </Button>]}
        >
          <Form autoComplete="off" className={styles.searchForm}>
            <Row>
              <Col span={6}>
                <FormattedMessage
                  id="app.components.resourceManage.dataTable.splitColumn"
                  defaultMessage="分片列"
                />: {splitColumn}
              </Col>
              <Col offset={18}>
                <Select
                  showSearch
                  optionFilterProp='children'
                  style={{minWidth: 240}}
                  onChange={this.handleChange}
                  placeholder='查询数据条数'
                  size="small"
                >
                  {numberList.map(item => (
                    <Option value={`${item}`} key={`${item}`}>
                      {`${item}`}
                    </Option>
                  ))}
                </Select>
              </Col>
            </Row>
          </Form>
          <Spin spinning={loading} tip="正在加载数据中...">
            {!loading ? (
              <Table
                style={{marginTop: 5}}
                size="small"
                rowKey={record => record.data}
                dataSource={dataSource}
                columns={columns}
                pagination={false}
                scroll={{x: true}}
              />
            ) : (
              <div style={{ height: '378px' }} />
            )}
          </Spin>
        </Modal>
      </div>
    )
  }
}

DataTableManageSourceInsightModal.propTypes = {
}
