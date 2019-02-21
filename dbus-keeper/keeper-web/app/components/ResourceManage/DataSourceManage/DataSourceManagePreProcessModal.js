import React, {PropTypes, Component} from 'react'
import {Button, Modal, Form, Select, Input, Upload, Icon, message} from 'antd'
const Dragger = Upload.Dragger;
import {
  DATA_SOURCE_PRE_PROCESS_API
} from '@/app/containers/ResourceManage/api'
import {FormattedMessage} from 'react-intl'
// 导入样式
import styles from './res/styles/index.less'

const FormItem = Form.Item
const Option = Select.Option
const TextArea = Input.TextArea

@Form.create()
export default class DataSourceManageBatchAddTableModal extends Component {
  constructor(props) {
    super(props)
    this.state = {
    }
  }

  render() {
    const {visible, key, onClose} = this.props

    const jarUploadProps = {
      name: 'templateFile',
      multiple: false,
      action: `${DATA_SOURCE_PRE_PROCESS_API}/1`,
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
        title='加表预处理'
        footer={[<Button type="primary" onClick={onClose}> 返 回 </Button>]}
      >
        <form id="preProcessForm" className={styles.preProcessForm}
              action={`${DATA_SOURCE_PRE_PROCESS_API}/1?token=${window.localStorage.getItem('TOKEN')}`} method="post"
              encType="multipart/form-data" target="hidden_frame"
              onChange={() => document.getElementById("preProcessForm").submit()}>
          <a className={styles.preProcessA} href='###' onClick={() => {
            document.getElementById("preProcessForm").reset()
            document.getElementById("preProcessFile").click()
          }}>
            上传Excel文件
            <input id="preProcessFile" className={styles.preProcessFile} type="file" name="templateFile"/>
          </a>
        </form>
      </Modal>
    )
  }
}

DataSourceManageBatchAddTableModal.propTypes = {}
