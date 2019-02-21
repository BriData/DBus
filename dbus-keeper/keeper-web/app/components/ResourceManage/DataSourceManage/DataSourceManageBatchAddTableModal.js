import React, {PropTypes, Component} from 'react'
import {Button, Modal, Form, Select, Input, Upload, Icon, message} from 'antd'
const Dragger = Upload.Dragger;
import {
  DATA_SOURCE_BATCH_ADD_TABLE_API
} from '@/app/containers/ResourceManage/api'
import {FormattedMessage} from 'react-intl'
import JSONTree from 'react-json-tree'
import dateFormat from "dateformat";
// 导入样式

const FormItem = Form.Item
const Option = Select.Option
const TextArea = Input.TextArea

@Form.create()
export default class DataSourceManageBatchAddTableModal extends Component {
  constructor(props) {
    super(props)
    this.state = {json: {} }
  }

  handleResultPayload = payload => {
    this.setState({json: payload})
  }

  render() {
    const {visible, key, onClose} = this.props
    const { json } = this.state
    const theme = {
      scheme: 'monokai',
      base00: '#272822'
    }
    const jarUploadProps = {
      name: 'uploadFile',
      multiple: false,
      action: DATA_SOURCE_BATCH_ADD_TABLE_API,
      data: { },
      headers: {
        'Authorization': window.localStorage.getItem('TOKEN')
      },
      onChange:(info)=>{
        console.info('info', info)
        console.info('info.event', info.event)
        const status = info.file.status;
        if (status === 'done') {
          if (info.file.response.status === 0) {
            message.success("批量加表成功");
            this.setState({json:info.file.response.payload});
          } else {
            message.warn("加表失败，" + info.file.response.message);
            this.setState({json:info.file.response.payload});
          }
        } else if (status === 'error') {
          message.error("文件上传失败！");
        }
      }
    }

    return (
      <Modal
        className="top-modal"
        visible={visible}
        maskClosable={false}
        key={key}
        onCancel={onClose}
        width={600}
        title='批量加表'
        footer={[<Button type="primary" onClick={onClose}> 返 回 </Button>]}
      >
        <Dragger {...jarUploadProps}>
          <p className="ant-upload-drag-icon">
            <Icon type="inbox" />
          </p>
          <p className="ant-upload-text">Click or drag CSV to this area to upload</p>
          <p className="ant-upload-hint">Support for a single upload</p>
        </Dragger>
        <JSONTree data={json} theme={theme} />
      </Modal>
    )
  }
}

DataSourceManageBatchAddTableModal.propTypes = {}
