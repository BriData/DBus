import React, { PropTypes, Component } from 'react'
import NewProjectCard from './NewProjectCard'
import ProjectCard from './ProjectCard'
import { Row, Col } from 'antd'
import {getUserInfo} from "@/app/utils/request";
const CARD_XL = 6
const CARD_SM = 8

export default class ProjectSummary extends Component {

  renderProjectCard (payload, onModifyProject, onEnableDisableProject, onDeleteProject) {
    // 504错误中的返回结果中有以下两个key
    if (payload.config && payload.response) return null
    let ret = []
    for (let key in payload) {
      let project = payload[key]
      ret.push(
        <Col key={project.id} xl={CARD_XL} sm={CARD_SM}>
          <ProjectCard projectBasicInfo={project} onModifyProject={onModifyProject} onEnableDisableProject={onEnableDisableProject} onDeleteProject={onDeleteProject} />
        </Col>
      )
    }
    return ret
  }

  render () {
    const userInfo = getUserInfo()

    const {onModifyProject, onEnableDisableProject, onDeleteProject, showModal, projectSummary} = this.props
    const payload = projectSummary.projectList.result
    return (
      <Row gutter={32} style={{margin: '8px 0'}}>
        {userInfo.roleType === 'admin' && (
          <Col xl={CARD_XL} sm={CARD_SM}>
            <NewProjectCard onShowModal={showModal} />
          </Col>
        )}
        {this.renderProjectCard(payload, onModifyProject, onEnableDisableProject, onDeleteProject)}
      </Row>
    )
  }
}

ProjectSummary.propTypes = {
  onModifyProject: PropTypes.func,
  onEnableDisableProject: PropTypes.func,
  showModal: PropTypes.func,
  onDeleteProject: PropTypes.func,
  projectSummary: PropTypes.object
}
