import React, { PropTypes, Component } from 'react'
import { Row, Col, Popconfirm, message, Icon } from 'antd'
import projectCardStyles from '../res/styles/projectSummary.less'
import { FormattedMessage } from 'react-intl'
import { getUserInfo } from '@/app/utils/request'
import moment from 'moment'
const MomentFormatString = "YYYY-MM-DD HH:mm:ss"
const RED = '#ff000047'
const YELLOW = '#ffc8002e'
const GRAY = '#e5e5e5'

const requireAll = x => x.keys().map(x)
const bgs = require.context('../res/image/background', false, /.*/)
const background = requireAll(bgs)

export default class ProjectCard extends Component {
  handleProjectClick = (userInfo, status, id, projectDisplayName) => {
    if (status === 'inactive') {
      message.error('项目不可用！')
      return
    }
    userInfo.roleType !== 'admin' && (window.location.href = `/project/table?projectId=${id}&projectDisplayName=${projectDisplayName}`)
  }
  render () {
    const userInfo = getUserInfo()
    const {projectBasicInfo, onModifyProject, onEnableDisableProject, onDeleteProject} = this.props
    const {id, project_icon: projectIcon, project_display_name: projectDisplayName, project_desc: projectDesc, project_expire: projectExpire} = projectBasicInfo

    const status = projectBasicInfo.status || 'active'
    const revertStatus = status === 'active' ? 'inactive' : 'active'
    const enableDisable = status === 'active' ? (
      <span>
          <Icon type="eye-o"/> <FormattedMessage id="app.components.projectManage.projectHome.card.active" defaultMessage="已生效" />
      </span>
    ) : (
      <span className={projectCardStyles.inactive}>
        <Icon type="eye"/> <FormattedMessage id="app.components.projectManage.projectHome.card.inactive" defaultMessage="已失效" />
      </span>
    )

    status === 'inactive' && (tip = "项目已失效")

    let style = {}
    userInfo.roleType !== 'admin' && (style = {
      ...style,
      cursor: "pointer"
    })
    // status === 'inactive' && (style = {
    //   ...style,
    //   backgroundColor: GRAY
    // })
    //
    let operatorStyle = {}
    // status === 'inactive' && (operatorStyle = {
    //   ...operatorStyle,
    //   backgroundColor: GRAY
    // })


    const now = moment()
    const expire = moment(projectExpire, MomentFormatString)
    const timeLeft = expire.diff(now, 'days')
    let tip = <FormattedMessage
      id="app.components.projectManage.projectHome.card.ExpireLeft"
      defaultMessage="有效期剩余: 0天"
      values={{day: timeLeft}}
    />
    if (timeLeft <= 0) {
      tip = <FormattedMessage id="app.components.projectManage.projectHome.card.projectExpire" defaultMessage="项目已过期" />
      // style = {
      //   ...style,
      //   backgroundColor: RED
      // }
      // operatorStyle = {
      //   ...operatorStyle,
      //   backgroundColor: RED
      // }
    } else if (timeLeft <= 30) {
      tip = <FormattedMessage id="app.components.projectManage.projectHome.card.projectWillExpire" defaultMessage="项目即将过期" />
      // style = {
      //   ...style,
      //   backgroundColor: YELLOW
      // }
      // operatorStyle = {
      //   ...operatorStyle,
      //   backgroundColor: YELLOW
      // }
    }

    return (
      <div className={projectCardStyles.projectCardOutline}>
        <div className={projectCardStyles.bg} style={{backgroundImage: `url(${background[id % background.length]})`}}/>
        <div className={projectCardStyles.summaryInfo} style={style} onClick={() => userInfo.roleType !== 'admin' && this.handleProjectClick(userInfo, status, id, projectDisplayName)}>
          <div className={projectCardStyles.titleAndContent}>
            <div className={projectCardStyles.title}>{projectDisplayName}</div>
            <div className={projectCardStyles.content}>{projectDesc}</div>
            <div className={projectCardStyles.expire}>{tip}</div>
            <div className={projectCardStyles.expire}>
              <FormattedMessage id="app.components.projectManage.projectHome.card.expireTime" defaultMessage="到期时间" />: {projectExpire}
            </div>
          </div>
        </div>
        <div className={projectCardStyles.operations} style={operatorStyle}>
          {userInfo.roleType === 'admin' ? (
            <Row>
              <Col span={8}>
                <div onClick={() => onModifyProject(id)} className={projectCardStyles.modify}>
                  <Icon type="edit"/> <FormattedMessage id="app.common.modify" defaultMessage="修改" />
                </div>
              </Col>
              <Col span={8}>
                <Popconfirm title={`${revertStatus}？`} onConfirm={() => onEnableDisableProject({...projectBasicInfo, status: revertStatus})} okText="Yes" cancelText="No">
                  <div className={projectCardStyles.disable}>{enableDisable}</div>
                </Popconfirm>

              </Col>
              <Col span={8}>
                <Popconfirm title={'确认删除？'} onConfirm={() => onDeleteProject(id)} okText="Yes" cancelText="No">
                  <div className={projectCardStyles.delete}>
                    <Icon type="delete" /> <FormattedMessage id="app.common.delete" defaultMessage="删除" />
                  </div>
                </Popconfirm>
              </Col>
            </Row>
          ) : (
            <Row>
              <Col span={12}>
                <div onClick={() => onModifyProject(id)} className={projectCardStyles.modify}>
                  <Icon type="edit" />
                  <FormattedMessage
                    id="app.common.modify"
                    defaultMessage="修改"
                  />
                </div>
              </Col>
              <Col span={12}>
                <div style={{cursor:'auto'}} className={projectCardStyles.disable}>{enableDisable}</div>
              </Col>
            </Row>
          )}
        </div>
      </div>
    )
  }
}

ProjectCard.propTypes = {
  projectBasicInfo: PropTypes.object,
  onModifyProject: PropTypes.func,
  onEnableDisableProject: PropTypes.func,
  onDeleteProject: PropTypes.func
}
