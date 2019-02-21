import React, {PropTypes, Component} from 'react'
import {Button, Modal, Form, Select, Input, Upload, Icon, message} from 'antd'
const Dragger = Upload.Dragger;
import {
  IMPORT_RULES_API
} from '@/app/containers/ResourceManage/api/index'


@Form.create()
export default class RuleImportModal extends Component {
  constructor(props) {
    super(props)
    this.state = {
    }
  }

  render() {
    const {visible, key, onClose, tableId} = this.props

    const jarUploadProps = {
      name: 'uploadFile',
      multiple: false,
      action: `${IMPORT_RULES_API}/${tableId}`,
      data: { },
      headers: {
        'Authorization': window.localStorage.getItem('TOKEN')
      },
      onChange(info) {
        console.info('info', info)
        const status = info.file.status;
        if (status === 'done') {
          if (info.file.response instanceof Object) {
            message.warn(info.file.response.message)
          } else {
            const url = window.URL.createObjectURL(new Blob([info.file.response]));
            const link = document.createElement('a');
            link.href = url;
            link.setAttribute('download', 'checkResult.zip');
            document.body.appendChild(link);
            link.click();
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
        title='上传规则'
        footer={[<Button type="primary" onClick={onClose}> 返 回 </Button>]}
      >
        <Dragger {...jarUploadProps}>
          <p className="ant-upload-drag-icon">
            <Icon type="inbox" />
          </p>
          <p className="ant-upload-text">Click or drag rules json to this area to upload</p>
          <p className="ant-upload-hint">Support for a single upload</p>
        </Dragger>
      </Modal>
    )
  }
}

RuleImportModal.propTypes = {}
