/**
 * @author 戎晓伟
 * @param className type:[Object string]      组件自定义类名
 * @param checkable   type:[Object Boolean]    选择项 true|false 多选|单选 默认 false
 * @param value    type:[Object Object]    传入的树型结构数据
 * @param searchValue type:[Object string]    传入的查询字符串     默认 ""
 * @param onLoadData  type:[Object Function]  动态加载树节点的promise方法
 * @param contextMenu type:[Object Object] 右键菜单 {type,title,onClick}
 * @description 公共组件 TreeList
 */
import React, { PropTypes, Component } from 'react'
import { fromJS, is } from 'immutable'
import { Tree, Icon } from 'antd'
import { ContextMenu, MenuItem, showMenu } from 'react-contextmenu'
// 导入样式表
import styles from './res/styles/index.less'
import './res/styles/contextMenu.css'

const TreeNode = Tree.TreeNode

export default class TreeList extends Component {
  constructor (props, context) {
    super(props, context)
    this.state = {
      expandedKeys: [],
      selectedKeys: [],
      treeData: [],
      searchValue: ''
    }
  }
  componentWillMount () {
    // 初始化接收数据
    this.setState({
      treeData: JSON.parse(JSON.stringify(this.props.value)),
      searchValue: this.props.searchValue || ''
    })
  }
  componentWillReceiveProps (nextProps) {
    // 接受传过来的数据
    let oldTreeData = fromJS(this.state.treeData)
    let newTreeData = fromJS(nextProps.value)
    let oldSearchValue = this.state.searchValue
    let newSearchValue = nextProps.searchValue
    // 判断是否重新接收数据渲染组件
    if (newSearchValue || newSearchValue === '') {
      // 如果有查询字符串 查询字符串相等以及传入的树型数据相等 不接收数据重新渲染组件
      if (newSearchValue === oldSearchValue && is(oldTreeData, newTreeData)) {
        return
      }
      // 否则接收传入的新数据
      this.setState({
        treeData: JSON.parse(JSON.stringify(nextProps.value)),
        searchValue: newSearchValue,
      })
    } else {
      // 如果查询字符串不存在  按树型结构数据判断  相同不接收，否则接收数据重新渲染
      if (is(oldTreeData, newTreeData)) {
        return
      }
      this.setState({
        treeData: JSON.parse(JSON.stringify(nextProps.value)),
        searchValue: newSearchValue,
      })
    }
  }
  /**
   * @description 展开/收起节点时触发
   */
  onExpand = (expandedKeys, nodeInfo) => {
    const path = nodeInfo.node.props.dataRef.path
    if (!nodeInfo.expanded) {
      if (path === '/') expandedKeys = []
      else expandedKeys = expandedKeys.filter(ek => (ek + '/').indexOf(path + '/') < 0)
    }
    this.setState({expandedKeys})
  }
  /**
   * @description 异步加载数据
   */

  onLoadData = treeNode => {
    const {loadLevelOfPath} = this.props
    return new Promise(resolve => {
      loadLevelOfPath(treeNode.props.dataRef.path)
      resolve()
    })
  }
  /**
   * @param data type:[Object Array]  生成新树的数据
   * @description 生成树结构
   */
  onCreateTree = searchValue => (data, parent) =>
    data.map(item => {
      const index = item['name'].indexOf(searchValue)
      const beforeStr = item['name'].substr(0, index)
      const afterStr = item['name'].substr(index + searchValue.length)
      const title = (
        <div
          title={item['name']}
          key={`${item['path']}`}
          style={{ display: 'inline-block' }}
          id={'treeList-context-menu'}
        >
          {index > -1 ? (
            <span className={styles.ellipsis}>
              {beforeStr}
              <span style={{ color: '#f50' }}>{searchValue}</span>
              {afterStr}
            </span>
          ) : (
            <span className={styles.ellipsis}>{item['name']}</span>
          )}
        </div>
      )
      // 子数据大于0 返回 子节点
      if (item.children) {
        return (
          <TreeNode
            key={`${item['path']}`}
            title={title}
            dataRef={item}
            parentRef={parent}
          >
            {this.onCreateTree(searchValue)(item.children, item)}
          </TreeNode>
        )
      }
      return (
        // 叶子节点
        <TreeNode
          key={`${item['path']}`}
          title={title}
          dataRef={item}
          parentRef={parent}
          isLeaf={!item.children}
        />
      )
    });

  /**
   * @description 根据传入的data返回新的Obj
   */

  handleGetValue = data => ({
    data_name: data.split('|')[0] || null,
    model_name: data.split('|')[1] || null
  });

  /**
   * @description 选择节点
   */
  handleSelect = (selectedKeys, e) => {
    if (e.selected) {
      this.setState({selectedKeys})
    }
    const {onReadData} = this.props
    onReadData(e.node.props.dataRef.path)
  }
  /**
   * @description 右键菜单
   */
  handleContextClick = e => {
    let showMenuConfig = {
      position: { x: e.event.pageX, y: e.event.pageY },
      id: 'treeList-context-menu',
      data: { treeNode: e.node }
    }
    showMenu(showMenuConfig)
  };

  /**
   * @description 设置treeData
   */
  handleSetTreeData = () => {
    this.setState({ treeData: [...this.state.treeData] })
  };

  render () {
    const {
      expandedKeys,
      selectedKeys,
      searchValue,
      treeData
    } = this.state
    const { contextMenu } = this.props
    return (
      <div className={styles['tree-list']}>
        {treeData && treeData.length > 0 ? (
          <div className={styles.wrapperTreeDiv}>
            <Tree
              loadData={this.onLoadData}
              expandedKeys={expandedKeys}
              selectedKeys={selectedKeys}
              onExpand={this.onExpand}
              multiple={this.props.checkable || false}
              onSelect={this.handleSelect}
              className={`${this.props.className || ''}`}
              showIcon={
                this.props.className && this.props.className === 'fileTree'
              }
              onRightClick={contextMenu && this.handleContextClick}
              showLine
            >
              {this.onCreateTree(searchValue)(treeData)}
            </Tree>
          </div>
        ) : (
          <div className={styles['noData']}>暂无数据</div>
        )}
        {contextMenu && (
          <ContextMenu id={'treeList-context-menu'}>
            {contextMenu &&
              contextMenu.length > 0 &&
              contextMenu.map((item, index) => (
                <MenuItem
                  key={item.key || `${index}`}
                  onClick={(e,data) => item.onClick(data)}
                >
                  {item.icon && (
                    <span className={styles.menuIcon}>
                      <Icon type={item.icon} />
                    </span>
                  )}
                  {item.title}
                </MenuItem>
              ))}
          </ContextMenu>
        )}
      </div>
    )
  }
}

// 判断组件属性是否传入符合要求
TreeList.propTypes = {
  className: PropTypes.string,
  checkable: PropTypes.bool,
  value: PropTypes.array,
  searchValue: PropTypes.string,
  contextMenu: PropTypes.array,
  onLoadData: PropTypes.func,
  onChange: PropTypes.func
}
