/**
 * @author xiancangao
 * @description
 */
import React, {Component} from 'react'
import {Form, Table, Tag, Tooltip} from 'antd'
import styles from './res/styles/index.less'
import {FormattedMessage} from 'react-intl'
import OperatingButton from '@/app/components/common/OperatingButton'

const FormItem = Form.Item
@Form.create({warppedComponentRef: true})
export default class OggCanalDeployForm extends Component {
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

  renderCanalStatus = (text, record, index) => {
    const {maxCanalNumber, deployCanalNumber} = record
    let color = 'green'
    if (maxCanalNumber <= deployCanalNumber) {
      color = 'red'
    }
    return (<Tag color={color}>
      {text}
    </Tag>)
  }

  renderReplicatStatus = (text, record, index) => {
    const {maxReplicatNumber, deployReplicatNumber} = record
    let color = 'green'
    if (maxReplicatNumber <= deployReplicatNumber) {
      color = 'red'
    }
    return (<Tag color={color}>
      {text}
    </Tag>)
  }

  renderOperating = (text, record, index) => {
    const {onModify} = this.props
    return (
      <div>
        <OperatingButton onClick={() => onModify(record)} icon="edit">
          <FormattedMessage id="app.common.modify" defaultMessage="修改"/>
        </OperatingButton>
      </div>
    )
  }

  render () {
    const formItemLayout = {
      labelCol: {span: 3},
      wrapperCol: {span: 23}
    }
    const {info} = this.props

    return (
      <div className={styles.table}>
        <Table
          size="small"
          rowKey={record => record.dsName}
          pagination={false}
          dataSource={info}
          columns={[
            {
              title: <FormattedMessage
                id="app.components.toolset.CanalOggDeployInfo.host"
                defaultMessage="host"
              />,
              dataIndex: 'host',
              key: 'host',
              width: '5%',
              render: this.renderComponent(this.renderNomal)
            },
            {
              title: <FormattedMessage
                id="app.components.toolset.CanalOggDeployInfo.canalPath"
                defaultMessage="canal小工具目录"
              />,
              dataIndex: 'canalPath',
              key: 'canalPath',
              width: '14%',
              render: this.renderComponent(this.renderNomal)
            },
            {
              title: <FormattedMessage
                id="app.components.toolset.CanalOggDeployInfo.maxCanalNumber"
                defaultMessage="canal最大部署数"
              />,
              dataIndex: 'maxCanalNumber',
              key: 'maxCanalNumber',
              width: '8%',
              render: this.renderComponent(this.renderNomal)
            },
            {
              title: <FormattedMessage
                id="app.components.toolset.CanalOggDeployInfo.deployCanalNumber"
                defaultMessage="canal实际部署数"
              />,
              dataIndex: 'deployCanalNumber',
              key: 'deployCanalNumber',
              width: '8%',
              render: this.renderComponent(this.renderCanalStatus)
            },
            {
              title: <FormattedMessage
                id="app.components.toolset.CanalOggDeployInfo.oggPath"
                defaultMessage="ogg根目录"
              />,
              dataIndex: 'oggPath',
              key: 'oggPath',
              width: '10%',
              render: this.renderComponent(this.renderNomal)
            },
            {
              title: <FormattedMessage
                id="app.components.toolset.CanalOggDeployInfo.oggToolPath"
                defaultMessage="ogg小工具目录"
              />,
              dataIndex: 'oggToolPath',
              key: 'oggToolPath',
              width: '10%',
              render: this.renderComponent(this.renderNomal)
            },
            {
              title: <FormattedMessage
                id="app.components.toolset.CanalOggDeployInfo.oggTrailPath"
                defaultMessage="oggTrailPath"
              />,
              dataIndex: 'oggTrailPath',
              key: 'oggTrailPath',
              width: '12%',
              render: this.renderComponent(this.renderNomal)
            },
            {
              title: <FormattedMessage
                id="app.components.toolset.CanalOggDeployInfo.mgrReplicatPort"
                defaultMessage="mgr进程端口号"
              />,
              dataIndex: 'mgrReplicatPort',
              key: 'mgrReplicatPort',
              width: '7%',
              render: this.renderComponent(this.renderNomal)
            },
            {
              title: <FormattedMessage
                id="app.components.toolset.CanalOggDeployInfo.maxReplicatNumber"
                defaultMessage="replicat最大部署数"
              />,
              dataIndex: 'maxReplicatNumber',
              key: 'maxReplicatNumber',
              width: '8%',
              render: this.renderComponent(this.renderNomal)
            },
            {
              title: <FormattedMessage
                id="app.components.toolset.CanalOggDeployInfo.deployReplicatNumber"
                defaultMessage="replicat实际部署数"
              />,
              dataIndex: 'deployReplicatNumber',
              key: 'deployReplicatNumber',
              width: '8%',
              render: this.renderComponent(this.renderReplicatStatus)
            },
            {
              title: (
                <FormattedMessage id="app.common.operate" defaultMessage="操作"/>
              ),
              render: this.renderComponent(this.renderOperating)
            }
          ]}
        />
      </div>
    )
  }
}

OggCanalDeployForm.propTypes = {}
