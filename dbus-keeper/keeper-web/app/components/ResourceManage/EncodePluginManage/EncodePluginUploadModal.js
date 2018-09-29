import React, { PropTypes, Component } from 'react'
import {Input, Form, Select,Icon,Button, Row, Col, Modal,Upload ,message} from 'antd'
const Dragger = Upload.Dragger;
import { FormattedMessage } from 'react-intl'
// 导入样式
import styles from './res/styles/index.less'

const FormItem = Form.Item
const Option = Select.Option



export default class EncodePluginUploadModal extends Component {
  constructor (props) {
    super(props)
    this.state = {
      name: '',
      projectId: '0'
    }
  }
  componentWillMount () {
  }

  handleNameChange = value => {
    this.setState({
      name: value
    })
  }

  handleProjectIdChange = value => {
    this.setState({
      projectId: value
    })
  }

  render () {
    const {visible, api, key, onClose, projectList} = this.props
    const {name, projectId} = this.state
    const projects = [{id:0, project_display_name: (<font color="blue">所有项目可用</font>)}, ...projectList]
    const jarUploadProps = {
      name: 'jarFile',
      multiple: false,
      action: `${api}/${name}/${projectId}`,
      data: { },
      headers: {
        'Authorization': window.localStorage.getItem('TOKEN')
      },
      beforeUpload: function () {
        if(!name) {
          message.error('请输入name')
          return false
        }
        if(!projectId && projectId !== '0') {
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
        title={<FormattedMessage
          id="app.components.projectManage.encodePlugin.uploadEncodePlugin"
          defaultMessage="上传脱敏插件"
        />}
        width={600}
        visible={visible}
        maskClosable={false}
        footer={[<Button type="primary" onClick={onClose}>
          <FormattedMessage
            id="app.common.back"
            defaultMessage="返回"
          />
        </Button>]}
        onCancel={onClose}>
        <div className={styles.searchForm}>
          <Row className={styles.rowSpacing}>
            <Col span={2} className={styles.formRight}>
              <FormattedMessage id="app.common.name" defaultMessage="名称" />:
            </Col>
            <Col span={8} offset={1}>
              <Input
                placeholder="Name"
                type="text"
                onChange={e => this.handleNameChange(e.target.value)}
                value={name}
              />
            </Col>
          </Row>
          <Row className={styles.rowSpacing}>
            <Col span={2} className={styles.formRight}>
              <FormattedMessage id="app.common.table.project" defaultMessage="项目" />:
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

EncodePluginUploadModal.propTypes = {
}
