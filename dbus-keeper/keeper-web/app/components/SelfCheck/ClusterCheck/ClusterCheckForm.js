/**
 * @author xiancangao
 * @description  基本信息设置
 */
import {FormattedMessage} from 'react-intl'
import React, {Component, PropTypes} from 'react'
import {Button, Col, Form, Icon, Input, Row, Select, Table, Tag, Tooltip} from 'antd'
import styles from './res/styles/index.less'

const TextArea = Input.TextArea
const Option = Select.Option
const FormItem = Form.Item
@Form.create({warppedComponentRef: true})
export default class ClusterCheckForm extends Component {
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

  handleCheckCluster = () => {
    this.props.form.validateFieldsAndScroll((err, values) => {
      if (!err) {
        const {onCheckCluster} = this.props
        onCheckCluster()
      }
    })
  }

  render () {
    const {getFieldDecorator} = this.props.form
    const formItemLayout = {
      labelCol: {span: 3},
      wrapperCol: {span: 18}
    }
    const tailFormItemLayout = {
      wrapperCol: {
        span: 18,
        offset: 3
      }
    }
    const titleItemLayout = {
      wrapperCol: {
        offset: 3,
        span: 18
      }
    }
    const {checkResult} = this.props
    const {grafanaUrl, influxdbUrl, heartBeatLeader, kafkaBrokers, nimbuses, supervisors, zkStats, loading} = checkResult
    return (
      <div className="form-search">
        <Form autoComplete="off" layout="horizontal">
          <FormItem {...tailFormItemLayout}>
            <Button loading={loading} type="primary" onClick={this.handleCheckCluster}>
              <FormattedMessage
                id="app.components.clusterCheck.clusterCheck.checkClusterStatus"
                defaultMessage="查询集群状态"
              />
            </Button>
          </FormItem>
          <FormItem {...titleItemLayout}>
            <Row>
              <Col span={3}>
                Grafana：
                {grafanaUrl === 'ok' ? (
                  <Icon style={{color: 'green'}} type="check-circle"/>
                ) : (
                  <Icon style={{color: 'red'}} type="close-circle"/>
                )}
              </Col>
              <Col span={3}>
                Influxdb：
                {influxdbUrl === 'ok' ? (
                  <Icon style={{color: 'green'}} type="check-circle"/>
                ) : (
                  <Icon style={{color: 'red'}} type="close-circle"/>
                )}
              </Col>
            </Row>
          </FormItem>
          <FormItem label={<FormattedMessage
            id="app.components.clusterCheck.clusterCheck.zkStatus"
            defaultMessage="ZK状态"
          />} {...formItemLayout}>
            <Table
              size="small"
              rowKey={record => `${record.host}:${record.port}`}
              pagination={false}
              dataSource={zkStats}
              columns={[
                {
                  title: <FormattedMessage
                    id="app.components.selfCheck.clusterCheck.host"
                    defaultMessage="主机"
                  />,
                  dataIndex: 'host',
                  key: 'host',
                  render: this.renderComponent(this.renderNomal)
                },
                {
                  title: <FormattedMessage
                    id="app.components.selfCheck.clusterCheck.port"
                    defaultMessage="端口"
                  />,
                  dataIndex: 'port',
                  key: 'port',
                  render: this.renderComponent(this.renderNomal)
                },
                {
                  title: <FormattedMessage
                    id="app.common.status"
                    defaultMessage="状态"
                  />,
                  dataIndex: 'state',
                  key: 'state',
                  render: this.renderComponent(this.renderStatus)
                }
              ]}
            />
          </FormItem>
          <FormItem label={<FormattedMessage
            id="app.components.clusterCheck.clusterCheck.heartbeatStatus"
            defaultMessage="心跳状态"
          />} {...formItemLayout}>
            <Table
              size="small"
              rowKey={record => record.name}
              pagination={false}
              dataSource={heartBeatLeader}
              columns={[
                {
                  title: <FormattedMessage
                    id="app.components.selfCheck.clusterCheck.host"
                    defaultMessage="主机"
                  />,
                  dataIndex: 'host',
                  key: 'host',
                  render: this.renderComponent(this.renderNomal)
                },
                {
                  title: 'pid',
                  dataIndex: 'pid',
                  key: 'pid',
                  render: this.renderComponent(this.renderNomal)
                },
                {
                  title: <FormattedMessage
                    id="app.common.status"
                    defaultMessage="状态"
                  />,
                  dataIndex: 'state',
                  key: 'state',
                  render: this.renderComponent(this.renderStatus)
                }
              ]}
            />
          </FormItem>
          <FormItem label={<FormattedMessage
            id="app.components.clusterCheck.clusterCheck.kafkaBrokerStatus"
            defaultMessage="Kafka集群状态"
          />} {...formItemLayout}>
            <Table
              size="small"
              rowKey={record => record.id}
              pagination={false}
              dataSource={kafkaBrokers}
              columns={[
                {
                  title: <FormattedMessage
                    id="app.components.selfCheck.clusterCheck.host"
                    defaultMessage="主机"
                  />,
                  dataIndex: 'host',
                  key: 'host',
                  render: this.renderComponent(this.renderNomal)
                },
                {
                  title: <FormattedMessage
                    id="app.components.selfCheck.clusterCheck.port"
                    defaultMessage="端口"
                  />,
                  dataIndex: 'port',
                  key: 'port',
                  render: this.renderComponent(this.renderNomal)
                },
                {
                  title: <FormattedMessage
                    id="app.common.status"
                    defaultMessage="状态"
                  />,
                  dataIndex: 'state',
                  key: 'state',
                  render: this.renderComponent(this.renderStatus)
                }
              ]}
            />
          </FormItem>

          <FormItem label="Storm Nimbus" {...formItemLayout}>
            <Table
              size="small"
              rowKey={record => record.host}
              pagination={false}
              dataSource={nimbuses}
              columns={[
                {
                  title: <FormattedMessage
                    id="app.components.selfCheck.clusterCheck.host"
                    defaultMessage="主机"
                  />,
                  dataIndex: 'host',
                  key: 'host',
                  render: this.renderComponent(this.renderNomal)
                },
                {
                  title: <FormattedMessage
                    id="app.components.selfCheck.clusterCheck.port"
                    defaultMessage="端口"
                  />,
                  dataIndex: 'port',
                  key: 'port',
                  render: this.renderComponent(this.renderNomal)
                },
                {
                  title: <FormattedMessage
                    id="app.common.status"
                    defaultMessage="状态"
                  />,
                  dataIndex: 'status',
                  key: 'status',
                  render: this.renderComponent(this.renderNomal)
                },
                {
                  title: <FormattedMessage
                    id="app.common.version"
                    defaultMessage="版本"
                  />,
                  dataIndex: 'version',
                  key: 'version',
                  render: this.renderComponent(this.renderNomal)
                },
                {
                  title: <FormattedMessage
                    id="app.components.selfCheck.clusterCheck.uptime"
                    defaultMessage="运行时间"
                  />,
                  dataIndex: 'nimbusUpTime',
                  key: 'nimbusUpTime',
                  render: this.renderComponent(this.renderNomal)
                }
              ]}
            />
          </FormItem>
          <FormItem label="Storm Supervisor" {...formItemLayout}>
            <Table
              size="small"
              rowKey={record => record.host}
              pagination={false}
              dataSource={supervisors}
              columns={[
                {
                  title: 'ID',
                  dataIndex: 'id',
                  key: 'id',
                  width: '15%',
                  render: this.renderComponent(this.renderNomal)
                },
                {
                  title: <FormattedMessage
                    id="app.components.selfCheck.clusterCheck.host"
                    defaultMessage="主机"
                  />,
                  dataIndex: 'host',
                  key: 'host',
                  render: this.renderComponent(this.renderNomal)
                },
                {
                  title: <FormattedMessage
                    id="app.components.selfCheck.clusterCheck.uptime"
                    defaultMessage="运行时间"
                  />,
                  dataIndex: 'uptime',
                  key: 'uptime',
                  render: this.renderComponent(this.renderNomal)
                },
                {
                  title: <FormattedMessage
                    id="app.components.selfCheck.clusterCheck.slots"
                    defaultMessage="槽数"
                  />,
                  dataIndex: 'slotsTotal',
                  key: 'slotsTotal',
                  render: this.renderComponent(this.renderNomal)
                },
                {
                  title: <FormattedMessage
                    id="app.components.selfCheck.clusterCheck.usedSlots"
                    defaultMessage="已使用槽数"
                  />,
                  dataIndex: 'slotsUsed',
                  key: 'slotsUsed',
                  render: this.renderComponent(this.renderNomal)
                },
                {
                  title: <FormattedMessage
                    id="app.components.selfCheck.clusterCheck.usedMem(MB)"
                    defaultMessage="已使用内存(兆)"
                  />,
                  dataIndex: 'usedMem',
                  key: 'usedMem',
                  render: this.renderComponent(this.renderNomal)
                },
                {
                  title: <FormattedMessage
                    id="app.common.version"
                    defaultMessage="版本"
                  />,
                  dataIndex: 'version',
                  key: 'version',
                  render: this.renderComponent(this.renderNomal)
                }
              ]}
            />
          </FormItem>
        </Form>
      </div>
    )
  }
}

ClusterCheckForm.propTypes = {
  form: PropTypes.object
}
