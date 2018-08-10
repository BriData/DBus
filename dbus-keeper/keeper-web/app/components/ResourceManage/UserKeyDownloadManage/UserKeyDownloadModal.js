import React, { PropTypes, Component } from 'react'
import {Input, Form, Select,Icon,Button, Row, Col, Modal,Upload ,message} from 'antd'
const Dragger = Upload.Dragger;
// 导入样式
import styles from './res/styles/index.less'

const FormItem = Form.Item
const Option = Select.Option



export default class UserKeyDownloadModal extends Component {
  constructor (props) {
    super(props)
    this.state = {
      principal: null,
      projectId: null
    }
  }
  componentWillMount () {
  }

  handlePrincipalChange = value => {
    this.setState({
      principal: value
    })
  }

  handleProjectIdChange = value => {
    this.setState({
      projectId: value
    })
  }

  render () {
    const {visible, api, key, onClose, projectList} = this.props
    const {principal, projectId} = this.state
    const projects = [...projectList]
    const jarUploadProps = {
      name: 'jarFile',
      multiple: false,
      action: `${api}/${projectId}/${principal}`,
      data: { },
      headers: {
        'Authorization': window.localStorage.getItem('TOKEN')
      },
      beforeUpload: function () {
        if(!principal) {
          message.error('请输入principal')
          return false
        }
        if(!projectId) {
          message.error('请选择project')
          return false
        }
        return true
      },
      onChange(info) {
        const status = info.file.status;
        if (status === 'done') {
          if (info.file.response.status === 0) message.success(info.file.response.message)
          else message.error(info.file.response.message)
        } else if (status === 'error') {
          message.error(info.file.name + " 上传失败！");
        }
      }
    }

    return (
      <Modal
        key={key}
        title="上传用户密钥"
        width={600}
        visible={visible}
        maskClosable={false}
        footer={[<Button type="primary" onClick={onClose}> 返 回 </Button>]}
        onCancel={onClose}>
        <div className={styles.searchForm}>
          <Row className={styles.rowSpacing}>
            <Col span={2} className={styles.formRight}>
              principal:
            </Col>
            <Col span={8} offset={1}>
              <Input
                placeholder="principal"
                type="text"
                onChange={e => this.handlePrincipalChange(e.target.value)}
                value={principal}
              />
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
                onChange={value => this.handleProjectIdChange(value)}
                value={projectId}
              >
                {projects.map(item => (
                  <Option value={`${item.id}`} key={`${item.id}`}>
                    {item.project_display_name}
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

UserKeyDownloadModal.propTypes = {
}
