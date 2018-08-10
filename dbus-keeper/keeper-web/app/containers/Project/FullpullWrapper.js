/**
 * @author 戎晓伟
 * @description  项目管理
 */

import React, { PropTypes, Component } from 'react'
import Helmet from 'react-helmet'

// 导入自定义组件
import ProjectFullpullWrapper from '../../containers/ProjectManage/ProjectFullpullWrapper'

export default class FullpullWrapper extends Component {
  constructor (props) {
    super(props)
    this.state = {
      query: {}
    }
  }
  componentWillMount = () => {
    const {location} = this.props
    const {query} = location
    if (Object.keys(query)) {
      this.setState({query})
    } else {
      window.location.href = '/project-manage/home'
    }
  }

  render () {
    const {query} = this.state
    return (
      <div>
        <Helmet
          title="Project11"
          meta={[{ name: 'description', content: 'Description of Project' }]}
        />
        <ProjectFullpullWrapper
          query={query}
        />
      </div>
    )
  }
}
