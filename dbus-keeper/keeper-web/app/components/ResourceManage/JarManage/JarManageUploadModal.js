import React, { PropTypes, Component } from 'react'
import { Form, Select,Icon,Button, Row, Col, Modal,Upload ,message} from 'antd'
const Dragger = Upload.Dragger;
import { FormattedMessage } from 'react-intl'
// 导入样式
import styles from './res/styles/index.less'
import {setToken} from "@/app/utils/request";
import Request from "@/app/utils/request";
import {
  JAR_GET_VERSION_LIST_API,
  JAR_GET_TYPE_LIST_API
} from '@/app/containers/ResourceManage/api'
const FormItem = Form.Item
const Option = Select.Option



export default class JarManageUploadModal extends Component {
  constructor (props) {
    super(props)
    this.state = {
      versionList: [],
      typeList: []
    }
  }

  componentWillMount () {
    this.getVersionList()
  }

  getVersionList = () => {
    Request(`${JAR_GET_VERSION_LIST_API}/${this.props.category}`, {
      method: 'get' })
      .then(res => {
        if (res && res.status === 0) {
          this.setState({
            versionList: res.payload
          })
        } else {
          message.warn(res.message)
        }
      })
      .catch(error => {
        error.response.data && error.response.data.message
          ? message.error(error.response.data.message)
          : message.error(error.message)
      })
  }

  handleVersionChange = value => {
    const {onChangeUploadParam} = this.props
    onChangeUploadParam({'uploadVersion': value,'uploadType': null})
    Request(`${JAR_GET_TYPE_LIST_API}`, {
      params: {
        category: this.props.category,
        version: value
      },
      method: 'get' })
      .then(res => {
        if (res && res.status === 0) {
          this.setState({
            typeList: res.payload
          })
        } else {
          message.warn(res.message)
        }
      })
      .catch(error => {
        error.response.data && error.response.data.message
          ? message.error(error.response.data.message)
          : message.error(error.message)
      })
  }

  render () {
    const {visible, api, onModalCancel, onChangeUploadParam} = this.props
    const {uploadVersion, uploadType, category} = this.props

    const {versionList, typeList} = this.state

    const jarUploadProps = {
      name: 'jarFile',
      multiple: false,
      action: `${api}/${uploadVersion}/${uploadType}/${category}`,
      data: { },
      headers: {
        'Authorization': window.localStorage.getItem('TOKEN')
      },
      beforeUpload: function () {
        if(!uploadVersion) {
          message.error('请选择Version')
          return false
        }
        if(!uploadType) {
          message.error('请选择Type')
          return false
        }
        return true
      },
      onChange(info) {
        const status = info.file.status;
        if (status === 'done') {
          message.success(info.file.name + " 上传成功！");
        } else if (status === 'error') {
          message.error(info.file.name + " 上传失败！");
        }
      }
    }

    return (
      <Modal
        title={<FormattedMessage
          id="app.components.resourceManage.jarManager.uploadJar"
          defaultMessage="上传Jar包"
        />}
        width={600}
        visible={visible}
        maskClosable={false}
        footer={[<Button type="primary" onClick={onModalCancel}> 返 回 </Button>]}
        onCancel={onModalCancel}>
        <div className={styles.searchForm}>
          <Row className={styles.rowSpacing}>
            <Col span={2} className={styles.formRight}>
              Version:
            </Col>
            <Col span={8} offset={1}>
              <Select
                showSearch
                optionFilterProp='children'
                className={styles.select}
                value={uploadVersion}
                onChange={value => this.handleVersionChange(value)}
              >
                {versionList.map(item => (
                  <Option value={item} key={`${item}`}>
                    {item}
                  </Option>
                ))}
              </Select>
            </Col>
          </Row>
          <Row className={styles.rowSpacing}>
            <Col span={2} className={styles.formRight}>
              Type:
            </Col>
            <Col span={8} offset={1}>
              <Select
                showSearch
                optionFilterProp='children'
                className={styles.select}
                value={uploadType}
                onChange={value => onChangeUploadParam({'uploadType': value})}
              >
                {typeList.map(item => (
                  <Option value={item} key={`${item}`}>
                    {item}
                  </Option>
                ))}
              </Select>
            </Col>
          </Row>
          <Row>
            <Col span={24}>
              <Dragger {...jarUploadProps}>
                <p className="ant-upload-drag-icon">
                  <Icon type="inbox" />
                </p>
                <p className="ant-upload-text">Click or drag file to this area to upload</p>
                <p className="ant-upload-hint">Support for a single upload</p>
              </Dragger>
            </Col>
          </Row>
        </div>
      </Modal>
    )
  }
}

JarManageUploadModal.propTypes = {
}
