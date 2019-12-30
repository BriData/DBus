import React, {Component, PropTypes} from 'react'
import {Form, Table, Tag, Tooltip} from 'antd'
import styles from './res/styles/index.less'

const FormItem = Form.Item
@Form.create({warppedComponentRef: true})
export default class OggCheckForm extends Component {
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

  renderOggStatus = (text, record, index) => {
    let color
    switch (text) {
      case 'RUNNING':
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
    const {oggStatus, loading} = this.props
    return (
      <div className="form-search">
        <FormItem {...formItemLayout}>
          <Table
            size="small"
            rowKey={record => record.dsName}
            pagination={false}
            dataSource={oggStatus}
            loading={loading}
            columns={[
              {
                title: 'dsName',
                dataIndex: 'dsName',
                key: 'dsName',
                width: '15%',
                render: this.renderComponent(this.renderNomal)
              },
              {
                title: '部署机器ip',
                dataIndex: 'host',
                key: 'host',
                render: this.renderComponent(this.renderNomal)
              },
              {
                title: 'replicatName',
                dataIndex: 'replicatName',
                key: 'replicatName',
                render: this.renderComponent(this.renderNomal)
              },
              {
                title: 'trailName',
                dataIndex: 'trailName',
                key: 'trailName',
                render: this.renderComponent(this.renderNomal)
              },
              {
                title: '状态',
                dataIndex: 'state',
                key: 'state',
                render: this.renderComponent(this.renderOggStatus)
              }
            ]}
          />
        </FormItem>
      </div>
    )
  }
}

OggCheckForm.propTypes = {
  form: PropTypes.object
}
