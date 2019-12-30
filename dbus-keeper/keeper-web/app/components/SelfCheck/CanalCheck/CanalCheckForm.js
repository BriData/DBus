import React, {Component, PropTypes} from 'react'
import {Form, Table, Tag, Tooltip} from 'antd'
import styles from './res/styles/index.less'

const FormItem = Form.Item
@Form.create({warppedComponentRef: true})
export default class CanalCheckForm extends Component {
  constructor (props) {
    super(props)
    this.state = {}
  }

  renderComponent = render => (text, record, index) =>
    render(text, record, index)

  /**
   * @description table默认的render
   */
  renderNomal = (text, record, index) => (
    <Tooltip title={text}>
      <div className={styles.ellipsis}>
        {text}
      </div>
    </Tooltip>
  )

  renderStatus = (text, record, index) => {
    let color
    switch (text) {
      case 'ok':
        color = 'green'
        break
      default:
        color = 'red'
    }
    return (<div title={text} className={styles.ellipsis}>
      <Tag color={color} style={{cursor: 'auto'}}>
        {text}
      </Tag>
    </div>)
  }

  render () {
    const formItemLayout = {
      labelCol: {span: 3},
      wrapperCol: {span: 18}
    }
    const {canalStatus, loading} = this.props
    return (
      <div className="form-search">
        <Form autoComplete="off" layout="horizontal">
          <FormItem {...formItemLayout}>
            <Table
              size="small"
              rowKey={record => record.dsName}
              pagination={false}
              dataSource={canalStatus}
              loading={loading}
              columns={[
                {
                  title: '数据源名称',
                  dataIndex: 'dsName',
                  key: 'dsName',
                  width: '15%',
                  render: this.renderComponent(this.renderNomal)
                },
                {
                  title: '部署机器地址',
                  dataIndex: 'host',
                  key: 'host',
                  render: this.renderComponent(this.renderNomal)
                },
                {
                  title: '备库地址',
                  dataIndex: 'canalAdd',
                  key: 'canalAdd',
                  render: this.renderComponent(this.renderNomal)
                },
                {
                  title: '进程号',
                  dataIndex: 'pid',
                  key: 'pid',
                  render: this.renderComponent(this.renderNomal)
                },
                {
                  title: 'slaveId',
                  dataIndex: 'slaveId',
                  key: 'slaveId',
                  render: this.renderComponent(this.renderNomal)
                },
                {
                  title: '状态',
                  dataIndex: 'state',
                  key: 'state',
                  render: this.renderComponent(this.renderStatus)
                }
              ]}
            />
          </FormItem>
        </Form>
      </div>
    )
  }
}

CanalCheckForm.propTypes = {
  form: PropTypes.object
}
