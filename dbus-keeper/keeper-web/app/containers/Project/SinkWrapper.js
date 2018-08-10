/**
 * @author 戎晓伟
 * @description  普通用户-项目管理-用户管理
 */

import React, { PropTypes, Component } from 'react'
import { connect } from 'react-redux'
import { createStructuredSelector } from 'reselect'
import Helmet from 'react-helmet'
// 导入自定义组件
import { SinkGrid, Bread } from '@/app/components'

// selectors
import { makeSelectLocale } from '../LanguageProvider/selectors'
import {ProjectHomeModel} from "@/app/containers/ProjectManage/selectors"
// action
import {} from './redux'
// 修改项目
import { getProjectInfo } from '@/app/containers/ProjectManage/redux'

// 链接reducer和action
@connect(
  createStructuredSelector({
    locale: makeSelectLocale(),
    projectHomeData: ProjectHomeModel()
  }),
  dispatch => ({
    getProjectInfo: param => dispatch(getProjectInfo.request(param))
  })
)
export default class SinkWrapper extends Component {
  constructor (props) {
    super(props)
    this.tableWidth = ['20%', '35%', '35%', '10%']
  }
  componentWillMount = () => {
    const { location, getProjectInfo} = this.props
    const projectId = location.query.projectId
    if (projectId) {
      getProjectInfo({ id: projectId })
      // this.handleSearch({projectId})
    } else {
      window.location.href = '/project-manage/home'
    }
  };
  /**
   * @param params 查询的参数 type:[Object Object]
   * @param boolean 是否将参数缓存起来 type:[Object Boolean]
   * @description 查询数据
   */
  handleSearch = params => {
    const { getSinkList } = this.props
    // 查询
    getSinkList(params)
  };
  render () {
    const breadSource = [
      {
        path: '/project-manage',
        name: 'home'
      },
      {
        path: '/project-manage',
        name: '项目管理'
      },
      {
        name: 'Sink'
      }
    ]
    this.props.location.query.projectDisplayName && breadSource.push({
      name: this.props.location.query.projectDisplayName
    })
    const {projectHomeData} = this.props
    const {projectInfo} = projectHomeData
    const {sinks} = projectInfo.result

    return (
      <div>
        <Helmet
          title="DbusKeeper"
          meta={[{ name: 'description', content: 'Description of Project' }]}
        />
        <Bread source={breadSource} />
        <SinkGrid sinkList={sinks} tableWidth={this.tableWidth} />
      </div>
    )
  }
}
