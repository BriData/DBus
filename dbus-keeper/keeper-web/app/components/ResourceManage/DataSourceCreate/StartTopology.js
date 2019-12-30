import React, {PropTypes, Component} from 'react'
import {Tag, Spin, Input, Form, Select, Table, Row, Col, Button, message} from 'antd'
import {FormattedMessage} from 'react-intl'
// 导入样式
const TextArea = Input.TextArea
import styles from './res/styles/index.less'
import Request from "@/app/utils/request";

import OperatingButton from '@/app/components/common/OperatingButton'

const FormItem = Form.Item
const Option = Select.Option

export default class StartTopology extends Component {
  constructor(props) {
    super(props)
    this.tableWidthStyle = width => ({
      width: `${parseFloat(width) / 100 * 1440}px`
    })
    this.tableWidth = [
      '10%',
      '10%',
      '10%',
      '10%',
      '16.6%',
      '16.6%',
      '16.7%',
      '10%',
    ]
    this.state = {
      dataSource: [],
      logContent: '',
      logLoading: false
    }
  }

  componentWillMount = () => {
    const {getLatestJarPath, dataSource} = this.props
    getLatestJarPath({dsId: dataSource.id})
  }

  componentWillReceiveProps = nextProps => {
    const {jarPath} = nextProps
    if (jarPath.length) {
      this.setState({
        dataSource: jarPath
      })
    }
  }

  handleStart = record => {
    const {logContent} = this.state
    this.setState({logLoading: true})
    const {topoJarStartApi} = this.props
    Request(topoJarStartApi, {
      data: {
        ...record,
        topologyType: record.topolotyType
      },
      method: 'post' })
      .then(res => {
        this.setState({logLoading: false})
        if (res && res.status === 0 && res.payload && res.payload.indexOf('Finished submitting topology') >= 0) {
          message.success(res.message)
          this.setState({logContent: logContent + res.payload})
          const {dataSource} = this.state
          this.setState({
            dataSource: dataSource.map(ds => {
              if (ds.topolotyName === record.topolotyName) {
                return {
                  ...ds,
                  status: 'running'
                }
              } else {
                return {
                  ...ds
                }
              }
            })
          })
        } else {
          this.setState({logContent: logContent + res.payload})
          message.warn(res.payload)
        }
      })
      .catch(error => {
        this.setState({logLoading: false})
        error.response.data && error.response.data.message
          ? message.error(error.response.data.message)
          : message.error(error.message)
      })
  }

  handleFinish = () => {
    const {dataSource} = this.state
    dataSource.forEach(ds => {
      if (ds.status === 'inactive') {
        message.warn(`${ds.topolotyName}没有启动`)
      }
    })

    if (dataSource.every(ds => ds.status === 'running')) {
      window.location.href='/resource-manage/data-source'
    }
  }

  renderComponent = render => (text, record, index) =>
    render(text, record, index);

  renderNomal = width => (text, record, index) => (
    <div
      title={text}
      style={this.tableWidthStyle(width)}
      className={styles.ellipsis}
    >
      {text}
    </div>
  )

  renderStatus = width => (text, record, index) => {
    if(text === 'inactive') text = 'stopped'
    let color
    switch (text) {
      case 'running':
        color = 'green'
        break
      case 'stopped':
        color = 'red'
        break
      default:
        color = '#929292'
    }
    return (<div style={this.tableWidthStyle(width)} title={text} className={styles.ellipsis}>
      <Tag color={color} style={{cursor: 'auto'}}>
        {text}
      </Tag>
    </div>)
  }

  renderOperating = width => (text, record, index) => {
    const {logLoading} = this.state
    return (
      <div style={this.tableWidthStyle(width)}>
        <OperatingButton disabled={logLoading || record.status === 'running'} icon="caret-right"
                         onClick={() => this.handleStart(record)}>启动</OperatingButton>
      </div>
    )
  }

  render() {
    const {logLoading, logContent, dataSource} = this.state
    const columns = [
      {
        title: 'dsName',
        width: this.tableWidth[0],
        dataIndex: 'dsName',
        key: 'dsName',
        render: this.renderComponent(this.renderNomal(this.tableWidth[0]))
      },
      {
        title: 'Topology Type',
        width: this.tableWidth[1],
        dataIndex: 'topolotyType',
        key: 'topolotyType',
        render: this.renderComponent(this.renderNomal(this.tableWidth[1]))
      },
      {
        title: 'Topology Name',
        width: this.tableWidth[2],
        dataIndex: 'topolotyName',
        key: 'topolotyName',
        render: this.renderComponent(this.renderNomal(this.tableWidth[2]))
      },
      {
        title: 'Status',
        width: this.tableWidth[3],
        dataIndex: 'status',
        key: 'status',
        render: this.renderComponent(this.renderStatus(this.tableWidth[3]))
      },
      {
        title: 'Jar Path',
        width: this.tableWidth[5],
        dataIndex: 'jarPath',
        key: 'jarPath',
        render: this.renderComponent(this.renderNomal(this.tableWidth[5]))
      },
      {
        title: 'Jar Name',
        width: this.tableWidth[6],
        dataIndex: 'jarName',
        key: 'jarName',
        render: this.renderComponent(this.renderNomal(this.tableWidth[6]))
      },
      {
        title: 'Operation',
        width: this.tableWidth[7],
        key: 'operation',
        render: this.renderComponent(this.renderOperating(this.tableWidth[7]))
      },
    ]
    return (
      <div className={styles.tableLayout}>
        <div className={styles.table}>
          <Table
            size="small"
            rowKey="topolotyName"
            dataSource={dataSource}
            columns={columns}
            pagination={false}
          />
        </div>
        <div style={{marginTop: 10}}>
          <Spin spinning={logLoading} tip="正在启动中...">
            <TextArea
              placeholder="storm log"
              autosize={{minRows: 10, maxRows: 20}}
              value={logContent}
              wrap='off'/>
          </Spin>
        </div>
        <div style={{marginTop: 10}}>
          <Row>
            <Col offset={22}>
              <Button
                type="primary"
                onClick={this.handleFinish}
              >
                加线完成
              </Button>
            </Col>
          </Row>
        </div>
      </div>
    )
  }
}

StartTopology.propTypes = {}
