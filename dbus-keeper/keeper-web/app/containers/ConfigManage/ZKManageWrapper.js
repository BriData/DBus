import React, {PropTypes, Component} from 'react'
import {connect} from 'react-redux'
import {createStructuredSelector} from 'reselect'
import Helmet from 'react-helmet'
import {makeSelectLocale} from '../LanguageProvider/selectors'
import {Row, Col,message} from 'antd'
import {
  Bread,
  FileTree,
  ZKManageContent
} from '@/app/components'
import Request from "@/app/utils/request";
import {
  LOAD_LEVEL_OF_PATH_API,
  ADD_NODE_API,
  DELETE_NODE_API
} from './api'
import {readZkData, saveZkData} from './redux'
import {ZKManageModel} from './selectors'
// 链接reducer和action
@connect(
  createStructuredSelector({
    locale: makeSelectLocale(),
    ZKManageData: ZKManageModel(),
  }),
  dispatch => ({
    readZkData: param => dispatch(readZkData.request(param)),
    saveZkData: param => dispatch(saveZkData.request(param)),
  })
)
export default class ZKManageWrapper extends Component {
  constructor(props) {
    super(props)
    this.state = {
      tree: [],
      contentKey: 'contentKey',
      currentEditPath: null
    }
  }

  componentWillMount() {
    this.loadLevelOfPath('/')
    this.handleReadData('/')
  }

  updateTree = payload => {
    const {tree} = this.state
    let root = tree[0]
    while (root.path !== payload.path) {
      let newRoot
      root.children.forEach(c => {
        if ((payload.path + '/').indexOf(c.path + '/') === 0) {
          newRoot = c
        }
      })
      root = newRoot
    }
    root.children = payload.children
    this.setState({tree})
  }

  loadLevelOfPath = (path) => {
    Request(LOAD_LEVEL_OF_PATH_API, {
      params: {
        path
      }
    })
      .then(res => {
        if (res && res.status === 0) {
          if(path === '/') {
            const tree = [res.payload]
            this.setState({tree})
          } else {
            this.updateTree(res.payload)
          }
        } else {
          message.warn(res.message)
        }
      })
      .catch(error => {
        error.response && error.response.data && error.response.data.message
          ? message.error(error.response.data.message)
          : message.error(error.message)
      })
  }

  handleReadData = path => {
    const {readZkData} = this.props
    readZkData({path})
    this.setState({
      currentEditPath: path,
      contentKey: this.handleRandom('contentKey')
    })
  }

  handleRandom = key =>
    `${Math.random()
      .toString(32)
      .substr(3, 8)}${key || ''}`;

  handleAddCallback = (data, nodeName) => {
    const {tree} = this.state
    let root = tree[0]
    const dataRef = data.treeNode.props.dataRef
    while (root.path !== dataRef.path) {
      let newRoot
      root.children.forEach(c => {
        if ((dataRef.path + '/').indexOf(c.path + '/') > -1) {
          newRoot = c
        }
      })
      root = newRoot
    }
    !root.children && (root.children = [])
    root.children.push({
      name: nodeName,
      path: root.path === '/' ? '/' + nodeName : root.path + '/' + nodeName
    })
    this.setState({tree})
  }

  handleAdd = (data, nodeName, cancelModal) => {
    const path = data.treeNode.props.dataRef.path
    Request(ADD_NODE_API, {
      params: {
        path,
        nodeName: path === '/' ? nodeName : '/' + nodeName
      }
    })
      .then(res => {
        if (res && res.status === 0) {
          this.handleAddCallback(data, nodeName)
          cancelModal()
          message.success(res.message)
        } else {
          message.warn(res.message)
        }
      })
      .catch(error => {
        error.response && error.response.data && error.response.data.message
          ? message.error(error.response.data.message)
          : message.error(error.message)
      })
  }

  handleDeleteCallback = (data) => {
    const {tree} = this.state
    let root = tree[0]
    const dataRef = data.treeNode.props.dataRef
    const parentRef = data.treeNode.props.parentRef
    while (root.path !== parentRef.path) {
      let newRoot
      root.children.forEach(c => {
        if ((parentRef.path + '/').indexOf(c.path + '/') > -1) {
          newRoot = c
        }
      })
      root = newRoot
    }
    root.children = root.children.filter(ch => {
      return ch.name !== dataRef.name
    })
    !root.children.length && (root.children = null)
    this.setState({tree})
  }

  handleDelete = (data, cancelModal) => {
    const path = data.treeNode.props.dataRef.path
    if (path === '/') {
      cancelModal()
      message.error('不允许删除根目录')
      return
    }
    Request(DELETE_NODE_API, {
      params: {
        path
      }
    })
      .then(res => {
        if (res && res.status === 0) {
          this.handleDeleteCallback(data)
          cancelModal()
          message.success(res.message)
        } else {
          message.warn(res.message)
        }
      })
      .catch(error => {
        error.response && error.response.data && error.response.data.message
          ? message.error(error.response.data.message)
          : message.error(error.message)
      })
  }

  handleRequest = (obj) => {
    const {api, params, data, method, callback, callbackParams} = obj
    Request(api, {
      params: {
        ...params
      },
      data: {
        ...data
      },
      method: method || 'get'
    })
      .then(res => {
        if (res && res.status === 0) {
          if (callback) {
            if (callbackParams) callback(...callbackParams)
            else callback()
          }
          message.success(res.message)
        } else {
          message.warn(res.message)
        }
      })
      .catch(error => {
        error.response && error.response.data && error.response.data.message
          ? message.error(error.response.data.message)
          : message.error(error.message)
      })
  }

  handleSave = (path, content) => {
    const {saveZkData} = this.props
    saveZkData({path, content})
  }

  render() {
    const breadSource = [
      {
        path: '/config-manage',
        name: 'home'
      },
      {
        path: '/config-manage',
        name: '配置中心'
      },
      {
        path: '/config-manage/zk-manage',
        name: 'ZK管理'
      }
    ]
    const {tree, currentEditPath, contentKey} = this.state
    const zkData = this.props.ZKManageData.zkData.result.payload
    console.info(this.props)
    return (
      <div>
        <Helmet
          title="数据源管理"
          meta={[
            {name: 'description', content: 'Description of DataSource Manage'}
          ]}
        />
        <Bread source={breadSource}/>

        <Row>
          <Col span={12}>
            <FileTree
              value={tree}
              loadLevelOfPath={this.loadLevelOfPath}
              onReadData={this.handleReadData}
              onAdd={this.handleAdd}
              onDelete={this.handleDelete}
            />
          </Col>
          <Col span={12}>
            <ZKManageContent
              key={contentKey}
              path={currentEditPath}
              zkData={zkData}
              onSave={this.handleSave}
            />
          </Col>
        </Row>
      </div>
    )
  }
}
ZKManageWrapper.propTypes = {
  locale: PropTypes.any
}
