/**
 * @author 戎晓伟
 * @description  普通用户-项目管理-表管理
 */

import React, { PropTypes, Component } from 'react'
import Helmet from 'react-helmet'

// 导入自定义组件
import ProjectTableWrapper from '../../containers/ProjectManage/ProjectTableWrapper'

export default class TableWrapper extends Component {
  constructor (props) {
    super(props)
    this.state = {
      projectId: '',
      projectDisplayName: ''
    }
  }
  componentWillMount = () => {
    const {location} = this.props
    const projectId = location.query.projectId
    const projectDisplayName = location.query.projectDisplayName
    if (projectId && projectDisplayName) {
      this.setState({projectId, projectDisplayName})
    } else {
      window.location.href = '/project-manage/home'
    }
  }

  handleViewFullPullHistory = (record) => {
    this.props.router.push({
      pathname: '/project/fullpull-history',
      query: {
        projectName: record.projectName,
        projectDisplayName: record.projectDisplayName,
        dsName: record.dsName,
        schemaName: record.schemaName,
        tableName: record.tableName,
        projectId: this.state.projectId
      }
    })
  }

  render () {
    const {projectId, projectDisplayName} = this.state
    return (
      <div>
        <Helmet
          title="Project11"
          meta={[{ name: 'description', content: 'Description of Project' }]}
        />
        <ProjectTableWrapper
          projectId={projectId}
          projectDisplayName={projectDisplayName}
          onViewFullPullHistory={this.handleViewFullPullHistory}
          isCreate
        />
      </div>
    )
  }
}
