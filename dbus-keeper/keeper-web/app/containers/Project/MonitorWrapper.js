/**
 * @author 戎晓伟
 * @description  项目管理
 */

import React, { PropTypes, Component } from 'react'
import Helmet from 'react-helmet'

// 导入自定义组件
import MonitorManageWrapper from '../../containers/MonitorManage/MonitorManageWrapper'

export default class MonitorWrapper extends Component {
  constructor (props) {
    super(props)
    this.state = {
      projectId: '',
      projectDisplayName: ''
    }
  }
  componentWillMount = () => {
    const {location} = this.props
    console.info(this.props)
    const projectId = location.query.projectId
    const projectDisplayName = location.query.projectDisplayName
    if (projectId && projectDisplayName) {
      this.setState({projectId, projectDisplayName})
    } else {
      window.location.href = '/project-manage/home'
    }
  }

  render () {
    const {projectId, projectDisplayName} = this.state
    return (
      <div style={{
        width: '100%',
        height: '100%',
      }}>
        <Helmet
          title="Project11"
          meta={[{ name: 'description', content: 'Description of Project' }]}
        />
        <MonitorManageWrapper
          projectId={projectId}
          projectDisplayName={projectDisplayName}
        />
      </div>
    )
  }
}
