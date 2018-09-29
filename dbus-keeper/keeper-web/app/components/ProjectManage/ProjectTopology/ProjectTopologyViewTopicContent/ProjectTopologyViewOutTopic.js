/**
 * @author 戎晓伟
 * @description  基本信息设置
 */

import React, {PropTypes, Component} from 'react'
import {Row, Col} from 'antd'
import { FormattedMessage } from 'react-intl'
export default class ProjectTopologyViewTopicContent extends Component {
  constructor(props) {
    super(props)
  }

  render() {
    const {item} = this.props
    const {sinkName, url, topics} = item
    return (
      <div>
        <Row>
          <Col span={4}>{<FormattedMessage id="app.components.sinkManage.sinkName" defaultMessage="Sink名称" />}</Col>
          <Col span={20}>{sinkName}</Col>
        </Row>
        <Row>
          <Col span={4}>{<FormattedMessage id="app.components.sinkManage.bootstrapServers" defaultMessage="Kafka服务器" />}</Col>
          <Col span={20}>{url}</Col>
        </Row>
        {topics.split(',').map((topic, index) =>
          index === 0 ?
            <Row key={`topic_${index}`}>
              <Col span={4}>{'Topics'}</Col>
              <Col style={{wordWrap: 'break-word', wordBreak: 'break-word'}} span={20}>{topic}</Col>
            </Row>
            :
            <Row key={`topic_${index}`}>
              <Col span={4} />
              <Col style={{wordWrap: 'break-word', wordBreak: 'break-word'}} span={20}>{topic}</Col>
            </Row>
        )}
        <br/>
      </div>
    )
  }
}

ProjectTopologyViewTopicContent.propTypes = {
  locale: PropTypes.any
}
