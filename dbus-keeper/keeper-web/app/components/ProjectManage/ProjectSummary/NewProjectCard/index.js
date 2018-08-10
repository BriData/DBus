import React, { Component, PropTypes } from 'react'
import projectCardStyles from '../res/styles/projectSummary.less'
import { FormattedMessage } from 'react-intl'

export default class NewProjectCard extends Component {
  render () {
    return (
      <div className={projectCardStyles.newProjectCardOutline}>
        <div className={projectCardStyles.newProjectCard}>
          <div onClick={() => this.props.onShowModal(true)}>
            <FormattedMessage
              id="app.components.projectManage.projectHome.card.newProject"
              defaultMessage="+ 新建项目"
            />
          </div>
        </div>
      </div>
    )
  }
}

NewProjectCard.propTypes = {
  onShowModal: PropTypes.func
}
