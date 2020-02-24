/**
 * @author 戎晓伟
 * @description  Sink信息设置
 */
import {FormattedMessage} from 'react-intl'
import React, {Component, PropTypes} from 'react'
import {Button, Input, Modal} from 'antd'
// 导入样式

const TextArea = Input.TextArea

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

  render() {
    const {visible, startTopoModalLog, loading, key} = this.props
    const {onClose} = this.props
    return (
      <Modal
        key={key}
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
