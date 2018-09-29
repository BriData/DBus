/**
 * @author 戎晓伟
 * @description  Sink信息设置
 */
import { FormattedMessage } from 'react-intl'
import React, { PropTypes, Component } from 'react'
import { Modal,Button, Input} from 'antd'
const TextArea = Input.TextArea
// 导入样式
import styles from './res/styles/index.less'

export default class ProjectTopologyStartModal extends Component {

  constructor(props) {
    super(props)
    this.textarea = null
  }

  textareaAutoScroll = () => {
    if (this.textarea) {
      this.textarea.textAreaRef.scrollTop = this.textarea.textAreaRef.scrollHeight
    }
  }

  componentDidUpdate = () => {
    setTimeout(this.textareaAutoScroll, 0)
  }

  render () {
    const { visible,startTopoModalLog, loading } = this.props
    const { onClose } = this.props
    return (
      <Modal
        closable={false}
        width={1000}
        title={<FormattedMessage
          id="app.common.log"
          defaultMessage="日志"
        />}
        visible={visible}
        footer={[<Button loading={loading} type="primary" onClick={onClose}>
          <FormattedMessage
            id="app.common.back"
            defaultMessage="返回"
          />
        </Button>]}
        maskClosable={false}
      >
        <TextArea ref={ref => (this.textarea = ref)} rows={12} value={startTopoModalLog} wrap='off'/>
      </Modal>
    )
  }
}

ProjectTopologyStartModal.propTypes = {
  local: PropTypes.any,
}
