import React, {PropTypes, Component} from 'react'
import {Modal, Form, Select, Input, Row, Col, Spin, message} from 'antd'
import {FormattedMessage} from 'react-intl'
import OperatingButton from '@/app/components/common/OperatingButton'
import DataTableManageVersionCompareTable from './DataTableManageVersionCompareTable'

// 导入样式
import styles from './res/styles/index.less'
import Request from "@/app/utils/request"

const FormItem = Form.Item
const Option = Select.Option
const TextArea = Input.TextArea

@Form.create()
export default class DataTableManageVersionModal extends Component {
  constructor(props) {
    super(props)
    this.state = {
      versionList: null,
      version1: null,
      version2: null,
    }
  }

  componentWillReceiveProps = nextProps => {
    const receivedVersionList = nextProps.versionList
    const {versionList} = this.state
    if (!versionList && receivedVersionList) {
      this.setState({versionList: receivedVersionList})
      if (receivedVersionList.length >= 2) {
        this.setState({
          version1: `${receivedVersionList[1].id}`,
          version2: `${receivedVersionList[0].id}`,
        })
      } else if (receivedVersionList.length === 1) {
        this.setState({
          version1: `${receivedVersionList[0].id}`,
          version2: `${receivedVersionList[0].id}`,
        })
      }
    }
  }

  handleCompareVersion = (version1, version2) => {
    const {getVersionDetail} = this.props
    getVersionDetail({
      versionId1: version1,
      versionId2: version2
    })
  }

  handleVersion1Change = value => {
    const {version2} = this.state
    this.handleCompareVersion(value, version2)
    this.setState({
      version1: value
    })
  }

  handleVersion2Change = value => {
    const {version1} = this.state
    this.handleCompareVersion(version1, value)
    this.setState({
      version2: value
    })
  }

  _rowEqual = (row1, row2) => {
    return row1.column_name === row2.column_name
  }

  _makeContent = detail => {
    if (!detail) return null
    const v1data = detail.v1data
    const v2data = detail.v2data
    // v1data和v2data从0开始
    // dp和path从1开始
    let i, j
    let dp = new Array(v1data.length + 1)
    for (i = 0; i <= v1data.length; i++) {
      dp[i] = new Array(v2data.length + 1)
      for (j = 0; j <= v2data.length; j++) {
        dp[i][j] = 0
      }
    }
    let path = new Array(v1data.length + 1)
    for (i = 0; i <= v1data.length; i++) {
      path[i] = new Array(v2data.length + 1)
      for (j = 0; j <= v2data.length; j++) {
        path[i][j] = 0
      }
    }

    const V1_LENGTH_SUB_1 = 1
    const V2_LENGTH_SUB_1 = 2
    const V1_AND_V2_LENGTH_SUB_1 = 3

    for (i = 1; i <= v1data.length; i++) {
      path[i][0] = V1_LENGTH_SUB_1
    }
    for (j = 1; j <= v2data.length; j++) {
      path[0][j] = V2_LENGTH_SUB_1
    }

    for (i = 1; i <= v1data.length; i++) {
      for (j = 1; j <= v2data.length; j++) {
        if (this._rowEqual(v1data[i - 1], v2data[j - 1])) {
          path[i][j] = V1_AND_V2_LENGTH_SUB_1
          dp[i][j] = dp[i - 1][j - 1] + 1
        } else {
          if (dp[i - 1][j] > dp[i][j - 1]) {
            path[i][j] = V1_LENGTH_SUB_1
            dp[i][j] = dp[i - 1][j]
          } else {
            path[i][j] = V2_LENGTH_SUB_1
            dp[i][j] = dp[i][j - 1]
          }
        }
      }
    }

    let content = []
    i = v1data.length
    j = v2data.length
    while (path[i][j] > 0) {
      if (path[i][j] === V1_LENGTH_SUB_1) {
        content.push({
          v1: v1data[i - 1],
          v2: null
        })
        i--
      } else if (path[i][j] === V2_LENGTH_SUB_1) {
        content.push({
          v1: null,
          v2: v2data[j - 1]
        })
        j--
      } else {
        content.push({
          v1: v1data[i - 1],
          v2: v2data[j - 1]
        })
        i--
        j--
      }
    }
    content.reverse()
    return content
  }

  renderComponent = render => (text, record, index) =>
    render(text, record, index)

  renderNomal = (text, record, index) => (
    <div title={text} className={styles.ellipsis}>
      {text}
    </div>
  )

  renderMiddleComments = (text, record, index) => (
    <div style={{borderRight: 1}} title={text} className={styles.ellipsis}>
      {text}
    </div>
  )

  render() {
    const columns = [
      {
        title: 'Name',
        dataIndex: 'name1',
        key: 'name1',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: 'Type',
        dataIndex: 'type1',
        key: 'type1',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: 'Length',
        dataIndex: 'length1',
        key: 'length1',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: 'Scale',
        dataIndex: 'scale1',
        key: 'scale1',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: 'Comments',
        dataIndex: 'comments1',
        key: 'comments1',
        render: this.renderComponent(this.renderMiddleComments)
      },
      {
        title: 'Name',
        dataIndex: 'name2',
        key: 'name2',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: 'Type',
        dataIndex: 'type2',
        key: 'type2',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: 'Length',
        dataIndex: 'length2',
        key: 'length2',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: 'Scale',
        dataIndex: 'scale2',
        key: 'scale2',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: 'Comments',
        dataIndex: 'comments2',
        key: 'comments2',
        render: this.renderComponent(this.renderNomal)
      },
    ]

    const {loading, key, visible, onClose} = this.props
    const {versionList, version1, version2} = this.state
    const {versionDetail} = this.props
    const content = this._makeContent(versionDetail)
    return (
      <div className={styles.table}>
        <Modal
          className="top-modal"
          key={key}
          visible={visible}
          maskClosable={false}
          width={1000}
          title={'表版本对比'}
          onCancel={onClose}
          onOk={onClose}
        >
          <Row>
            <Col span={2}>
              <div style={{fontSize: 14, textAlign: 'right'}}>版本1：</div>
            </Col>
            <Col span={9}>
              <Select
                showSearch
                optionFilterProp='children'
                style={{width: '100%'}}
                className={styles.select}
                onChange={this.handleVersion1Change}
                value={version1 && version1}
              >
                {versionList && versionList.map(item => (
                  <Option value={`${item.id}`} key={`${item.id}`}>
                    {`id:${item.id},ver:${item.version},inVer:${item.innerVersion},time:${new Date(item.updateTime).toLocaleString()}`}
                  </Option>
                ))}
              </Select>
            </Col>

            <Col span={2}>
              <div style={{fontSize: 14, textAlign: 'right'}}>版本2：</div>
            </Col>
            <Col span={9}>
              <Select
                showSearch
                optionFilterProp='children'
                style={{width: '100%'}}
                className={styles.select}
                onChange={this.handleVersion2Change}
                value={version2 && version2}
              >
                {versionList && versionList.map(item => (
                  <Option value={`${item.id}`} key={`${item.id}`}>
                    {`id:${item.id},ver:${item.version},inVer:${item.innerVersion},time:${new Date(item.updateTime).toLocaleString()}`}
                  </Option>
                ))}
              </Select>
            </Col>
          </Row>
          <Spin spinning={loading} tip="正在加载数据中...">
            {!loading ? (
              <DataTableManageVersionCompareTable
                content={content}
              />
            ) : (
              <div style={{width: '100%', height: 100}}/>
            )}
          </Spin>

        </Modal>
      </div>
    )
  }
}

DataTableManageVersionModal.propTypes = {}
