/**
 * @author 戎晓伟
 * @description  Sink信息设置
 */

import React, { PropTypes, Component } from 'react'
import { Modal, Input } from 'antd'
import TreeList from '../TreeList/index'
// 导入样式
import './res/styles/fileTree.css'

export default class FileTree extends Component {
  constructor (props) {
    super(props)
    this.state = {
      data: null,
      visible: false,
      type: 'add',
      title: '添加节点',
      inputValue: ''
    }
  }
  /**
   * @param data treeNode节点
   * @param type 右键菜单类型
   * @param callback treeNode ContextMenu回调
   * @description 根据不同的状态处理ContextMenu
   */
  handleShowContextMenu = (data, type) => {
    switch (type) {
      case 'add':
        this.handleModelState(data, 'add', '添加节点')
        break
      case 'delete':
        this.handleModelState(data, 'delete', '删除节点')
        break
      default:
        break
    }
  };
  /**
   * @param visible 弹窗状态
   * @description 弹窗的显示和隐藏
   */
  handleModelVisible = visible => {
    this.setState({ visible })
  };
  /**
   * @param type 弹窗类型
   * @description 弹窗显示并设置弹窗类型
   */
  handleModelState = (data, type, title) => {
    this.handleModelVisible(true)
    this.setState({data, type, title })
  };
  /**
   * @description input Change
   */
  handleChange = e => {
    const inputValue = e.target.value
    this.setState({ inputValue })
  };
  /**
   * @description 弹出层提交代码
   */
  handleOk = () => {
    const {onAdd, onDelete} = this.props
    const {data, type, inputValue } = this.state
    switch (type) {
      case 'add':
        onAdd(data, inputValue, this.handleCancel)
        break
      case 'delete':
        onDelete(data, this.handleCancel)
        break
    }
  };
  /**
   * @description 关闭弹出层
   */
  handleCancel = () => {
    this.handleModelVisible(false)
    this.setState({ inputValue: '' })
  };
  render () {
    const { visible, title, inputValue, type } = this.state
    const { value, loadLevelOfPath, onReadData } = this.props
    return (
      <div>
        <TreeList
          loadLevelOfPath={loadLevelOfPath}
          onReadData={onReadData}
          className="fileTree"
          searchValue=""
          contextMenu={[
            {
              title: '添加节点',
              type: 'add',
              icon: 'plus',
              onClick: (data) =>
                this.handleShowContextMenu(data, 'add')
            },
            {
              title: '删除节点',
              icon: 'delete',
              type: 'delete',
              onClick: (data) =>
                this.handleShowContextMenu(data, 'delete')
            }
          ]}
          value={value}
        />
        <Modal
          className="small-modal"
          title={title}
          visible={visible}
          width="240px"
          onOk={this.handleOk}
          onCancel={this.handleCancel}
          okText="确认"
          cancelText="取消"
        >
          {type === 'delete' ? (
            <p style={{ padding: '8px' }}>确认删除此节点？</p>
          ) : (
            <Input
              onPressEnter={this.handleOk}
              size="large"
              value={inputValue}
              onChange={this.handleChange}
            />
          )}
        </Modal>
      </div>
    )
  }
}

FileTree.propTypes = {
  local: PropTypes.any
}
