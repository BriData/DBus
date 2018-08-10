/**
 * @author 戎晓伟
 * @description  Sink信息设置
 */

import React, {PropTypes, Component} from 'react'
import {Modal, Card} from 'antd'
import ProjectTopologyViewInTopic from './ProjectTopologyViewTopicContent/ProjectTopologyViewInTopic'
import ProjectTopologyViewOutTopic from './ProjectTopologyViewTopicContent/ProjectTopologyViewOutTopic'
// 导入样式
import styles from './res/styles/index.less'

export default class ProjectTopologyViewTopicModal extends Component {

  constructor(props) {
    super(props)
  }

  render() {
    const {visible, topoName, type} = this.props
    const title = type === 'source' ? `${topoName}订阅的源Topic列表` : `${topoName}输出的Topic列表`

    const {inTopic, outTopic} = this.props
    const content = type === 'source' ? Object.values(inTopic.result) : Object.values(outTopic.result)

    const {onClose} = this.props
    return (
      <Modal
        width={1000}
        title={title}
        visible={visible}
        onCancel={onClose}
        onOk={onClose}
        maskClosable={false}
      >
        {
          type === 'source'
          ?
            content.map((item,index) => <ProjectTopologyViewInTopic key={`topic_info_${index}`} item={item}/>)
          :
            content.map((item,index) => <ProjectTopologyViewOutTopic key={`topic_info_${index}`} item={item}/>)
        }
      </Modal>
    )
  }
}

ProjectTopologyViewTopicModal.propTypes = {
  local: PropTypes.any,
}
