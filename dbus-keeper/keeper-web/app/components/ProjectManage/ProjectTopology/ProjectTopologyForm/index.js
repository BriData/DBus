/**
 * @author 戎晓伟
 * @description  基本信息设置
 */

import React, { PropTypes, Component } from 'react'
import { Modal, message, Spin } from 'antd'
import Request, {setToken} from '@/app/utils/request'
// 导入API
import {
  ADD_TOPOLOGY_API,
  MODIFY_TOPOLOGY_API
} from '@/app/containers/ProjectManage/api'

// 导入子组件
import ProjectTopologyInfo from './ProjectTopologyInfo'

export default class ProjectTopologyForm extends Component {
  constructor (props) {
    super(props)
    this.state = {
      loading: false
    }
  }

  /**
   * @deprecated 提交数据
   */
  handleSubmit = () => {
    const {
      modalStatus,
      topologyInfo,
      onCloseModal,
      onSearch,
      topologyParams,
      projectId
    } = this.props
    const { result } = topologyInfo
    const requestAPI =
      modalStatus === 'create' ? ADD_TOPOLOGY_API : MODIFY_TOPOLOGY_API
    this.topoInfoRef.validateFieldsAndScroll((err, values) => {
      if (!err) {
        let param =
          modalStatus === 'create'
            ? { ...values, projectId, status: 'new' }
            : { ...result, ...values, projectId, status: 'changed' }
        this.setState({ loading: true })
        Request(requestAPI, {
          data: param,
          method: 'post'
        })
          .then(res => {
            if (res && res.status === 0) {
              onCloseModal(false)
              onSearch(topologyParams, false)
            } else {
              message.warn(res.message)
            }
            this.setState({ loading: false })
          })
          .catch(error => {
            error.response.data && error.response.data.message
              ? message.error(error.response.data.message)
              : message.error(error.message)
            this.setState({ loading: false })
          })
      }
    })
  };

  render () {
    const { loading } = this.state
    const {
      getTopologyTemplateApi,
      visibal,
      locale,
      topologyInfo,
      modalStatus,
      modalKey,
      projectId,
      packages,
      versions,
      onCloseModal,
      onGetJarVersions,
      onGetJarPackages
    } = this.props
    const {result} = topologyInfo
    return (
      <Modal
        key={modalKey}
        visible={visibal}
        maskClosable={false}
        width={'800px'}
        style={{ top: 60 }}
        onCancel={() => onCloseModal(false)}
        onOk={this.handleSubmit}
        confirmLoading={loading}
        title={modalStatus === 'modify' ? '修改Topology' : '新增Topology'}
      >
        {
          <Spin spinning={topologyInfo.loading} tip="正在加载数据中...">
            {!topologyInfo.loading ? (
              <ProjectTopologyInfo
                ref={ref => (this.topoInfoRef = ref)}
                locale={locale}
                modalStatus={modalStatus}
                packages={packages}
                versions={versions}
                topologyInfo={result}
                projectId={projectId}
                onGetJarVersions={onGetJarVersions}
                onGetJarPackages={onGetJarPackages}
                getTopologyTemplateApi={getTopologyTemplateApi}
        />
            ) : (
              <div style={{ height: '378px' }} />
            )}
          </Spin>
      }
      </Modal>
    )
  }
}

ProjectTopologyForm.propTypes = {
  locale: PropTypes.any,
  modalKey: PropTypes.string,
  visibal: PropTypes.bool,
  projectId: PropTypes.string,
  topologyInfo: PropTypes.object,
  modalStatus: PropTypes.string,
  packages: PropTypes.object,
  versions: PropTypes.object,
  onCloseModal: PropTypes.func,
  onGetJarVersions: PropTypes.func,
  onGetJarPackages: PropTypes.func,
  onSearch: PropTypes.func,
  topologyParams: PropTypes.object
}
