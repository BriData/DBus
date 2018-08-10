/**
 * @author 戎晓伟
 * @description  项目管理-（新建,修改组件）
 */

import React, { PropTypes, Component } from 'react'
import { FormattedMessage } from 'react-intl'
import { Tabs, Icon } from 'antd'
// 导入自定义组件
import AlarmForm from './ProjectEditedTabs/AlarmForm'
import BasicInfoForm from './ProjectEditedTabs/BasicInfoForm'
import ResourceForm from './ProjectEditedTabs/ResourceForm'
import SinkForm from './ProjectEditedTabs/SinkForm'
import UserForm from './ProjectEditedTabs/UserForm'
// 导入样式
import styles from './res/styles/index.less'

const TabPane = Tabs.TabPane
// 常量
const SELECTED_TABLE_SCROLL_Y = 157

export default class ProjectEditedModal extends Component {
  constructor (props) {
    super(props)
    this.state = {
      alarmFlag: false
    }
    this.tableWidthStyle = width => ({
      width: `${parseFloat(width) / 100 * 960 - 15}px`
    })
  }
  componentWillMount () {
    const { modalStatus, projectHomeData } = this.props
    const { projectStorage, projectInfo } = projectHomeData
    const { result, loaded } = projectInfo
    const { alarm } = projectStorage
    if (!alarm) {
      const newAlarm = {
        schemaChangeNotify: { checked: false, value: [] },
        slaveSyncDelayNotify: { checked: false, value: [] },
        fullpullNotify: { checked: false, value: [] },
        dataDelayNotify: { checked: false, value: [] }
      }
      this.props.setAlarm(newAlarm)
    }
    // 编辑状态时 回显内容
    modalStatus === 'modify' && loaded && this.handleCreateData(result)
  }
  /**
   * @param params [object Object] 接收原始数据
   * @description 重新组装数据并存储到redux
   */
  handleCreateData = params => {
    // 组装数据
    let basic, user, sink, resource, alarm, encodes
    let temporaryData = {}
    const {
      setBasicInfo,
      setUser,
      setSink,
      setResource,
      setAlarm,
      setEncodes
    } = this.props
    // basic
    basic = {}
    basic['id'] = params['project']['id']
    basic['projectDesc'] = params['project']['projectDesc']
    basic['projectDisplayName'] = params['project']['projectDisplayName']
    basic['projectName'] = params['project']['projectName']
    basic['projectOwner'] = params['project']['projectOwner']
   // basic['allowAdminManage'] = params['project']['allowAdminManage']
    basic['projectExpire'] = params['project']['projectExpire']
    basic['topologyNum'] = params['project']['topologyNum']
    temporaryData['basic'] = basic
    setBasicInfo(basic)
    // user
    user = this.handleCreateArrayToMap(params['users'])
    temporaryData['user'] = user
    setUser(user)
    // sink
    sink = this.handleCreateArrayToMap(params['sinks'])
    temporaryData['sink'] = sink
    setSink(sink)
    // resource
    resource = this.handleCreateArrayToMap(params['resources'])
    temporaryData['resource'] = resource
    setResource(resource)
    // alarm
    alarm = {
      schemaChangeNotify: this.handleCreateAlarmItem(
        params['project'],
        'schemaChangeNotify'
      ),
      slaveSyncDelayNotify: this.handleCreateAlarmItem(
        params['project'],
        'slaveSyncDelayNotify'
      ),
      fullpullNotify: this.handleCreateAlarmItem(
        params['project'],
        'fullpullNotify'
      ),
      dataDelayNotify: this.handleCreateAlarmItem(
        params['project'],
        'dataDelayNotify'
      )
    }
    temporaryData['alarm'] = alarm
    setAlarm(alarm)
    // encodes
    encodes = {}
    params['encodes'] && Object.keys(params['encodes']).length > 0
      ? Object.entries(params['encodes']).forEach(item => {
        encodes[`${item[0]}`] = this.handleCreateArrayToMap(item[1], 'cid')
      })
      : (encodes = null)
    temporaryData['encodes'] = encodes
    setEncodes(encodes)
  };
  /**
   * @param alarm  [object object] 报警Object
   * @param alarmItem [object string] 报警的前缀key
   * @description 重新生成一个{checked,value} obj
   */
  handleCreateAlarmItem = (alarm, alarmItem) => {
    const temporaryAlarmItem = {}
    temporaryAlarmItem['checked'] = Boolean(alarm[`${alarmItem}Flag`])
    temporaryAlarmItem['value'] =
      alarm[`${alarmItem}Emails`] &&
      alarm[`${alarmItem}Emails`]
        .split(',')
        .map(item => ({ name: item, email: item }))
    return temporaryAlarmItem
  };
  /**
   * @param param  [object object] 报警Object
   * @description 将数组重新生成一个 子项为 {key,value}的object
   */
  handleCreateArrayToMap = (param, name) => {
    const temporaryParam = {}
    param.forEach(item => {
      temporaryParam[`_${name ? item[`${name}`] : item.id}`] =
        name === 'cid'
          ? { ...item, tableId: item.tid, fieldName: item.columnName }
          : item
    })
    return temporaryParam
  };
  /**
   * @param alarmFlag [object Boolean] 判断报警选项卡是否已编辑状态
   * @description 修改判断报警状态
   */
  handleChangeAlarmFlag = alarmFlag => {
    this.setState({ alarmFlag })
  };
  /**
   * @param text [object String] 弹出层标题
   * @description Modal 标题
   */
  renderModalTitle = text => (
    <div title={text} className={styles.tabBarTitle}>
      {text}
    </div>
  );
  render () {
    const {
      errorFlag,
      projectHomeData,
      modalActiveTab,
      modalStatus,
      onChangeTabs,
      setBasicInfo,
      setUser,
      setSink,
      setResource,
      setAlarm,
      setEncodes,
      locale,
      setUserParams,
      setSinkParams,
      setResourceParams,
      searchUser,
      searchSink,
      searchResource,
      searchEncode,
      getEncodeTypeList,
      isUserRole,
    } = this.props
    const {
      projectStorage,
      userList,
      sinkList,
      resourceList,
      userParams,
      sinkParams,
      resourceParams,
      encodeList,
      encodeTypeList
    } = projectHomeData
    const { alarmFlag } = this.state
    return (
      <div className="project-modal-tabs">
        <Tabs
          defaultActiveKey="basic"
          activeKey={modalActiveTab}
          onTabClick={onChangeTabs}
        >
          <TabPane
            tab={
              <span>
                <Icon type="appstore-o" />
                {
                  <FormattedMessage
                    id="app.components.projectManage.projectHome.tabs.basic"
                    defaultMessage="基本信息"
                  />
                }
              </span>
            }
            key="basic"
          >
            <BasicInfoForm
              ref="basicFormRef"
              basicInfo={projectStorage.basic}
              setBasicInfo={setBasicInfo}
              modalStatus={modalStatus}
              locale={locale}
              isUserRole={isUserRole}
            />
          </TabPane>
          <TabPane
            tab={
              <span>
                <Icon type="solution" />
                {
                  <FormattedMessage
                    id="app.components.projectManage.projectHome.tabs.user"
                    defaultMessage="用户管理"
                  />
                }
              </span>
            }
            key="user"
          >
            <UserForm
              locale={locale}
              errorFlag={errorFlag}
              user={projectStorage.user}
              setUser={setUser}
              setAlarm={setAlarm}
              alarmFlag={alarmFlag}
              modalStatus={modalStatus}
              tableWidthStyle={this.tableWidthStyle}
              selectedTableScrollY={SELECTED_TABLE_SCROLL_Y}
              userList={userList}
              userParams={userParams}
              onSetParams={setUserParams}
              onSearchList={searchUser}
              isUserRole={isUserRole}
            />
          </TabPane>
          <TabPane
            tab={
              <span>
                <Icon type="schedule" />
                {
                  <FormattedMessage
                    id="app.components.projectManage.projectHome.tabs.sink"
                    defaultMessage="Sink管理"
                  />
                }
              </span>
            }
            key="sink"
          >
            <SinkForm
              locale={locale}
              errorFlag={errorFlag}
              sink={projectStorage.sink}
              setSink={setSink}
              sinkList={sinkList}
              sinkParams={sinkParams}
              onSetParams={setSinkParams}
              onSearchList={searchSink}
              tableWidthStyle={this.tableWidthStyle}
              selectedTableScrollY={SELECTED_TABLE_SCROLL_Y}
              isUserRole={isUserRole}
            />
          </TabPane>
          <TabPane
            tab={
              <span>
                <Icon type="hdd" />
                {
                  <FormattedMessage
                    id="app.components.projectManage.projectHome.tabs.resource"
                    defaultMessage="Resource管理"
                  />
                }
              </span>
            }
            key="resource"
          >
            <ResourceForm
              locale={locale}
              errorFlag={errorFlag}
              resource={projectStorage.resource}
              encodes={projectStorage.encodes}
              basic={projectStorage.basic}
              encodeTypeList={encodeTypeList}
              setResource={setResource}
              setEncodes={setEncodes}
              getEncodeTypeList={getEncodeTypeList}
              resourceList={resourceList}
              resourceParams={resourceParams}
              onSetParams={setResourceParams}
              onSearchList={searchResource}
              encodeList={encodeList}
              onSearchEncode={searchEncode}
              tableWidthStyle={this.tableWidthStyle}
              selectedTableScrollY={SELECTED_TABLE_SCROLL_Y}
              isUserRole={isUserRole}
            />
          </TabPane>
          <TabPane
            tab={
              <span>
                <Icon type="exclamation-circle-o" />
                {
                  <FormattedMessage
                    id="app.components.projectManage.projectHome.tabs.alarm"
                    defaultMessage="报警策略设置"
                  />
                }
              </span>
            }
            key="alarm"
          >
            <AlarmForm
              ref="alarmFormRef"
              alarm={projectStorage.alarm}
              onChangeAlarmFlag={this.handleChangeAlarmFlag}
              user={
                projectStorage.user ? Object.values(projectStorage.user) : null
              }
              setAlarm={setAlarm}
              isUserRole={isUserRole}
            />
          </TabPane>
        </Tabs>
      </div>
    )
  }
}

ProjectEditedModal.propTypes = {
  locale: PropTypes.any,
  errorFlag: PropTypes.string,
  projectHomeData: PropTypes.object,
  modalActiveTab: PropTypes.string,
  modalStatus: PropTypes.string,
  onChangeTabs: PropTypes.func,
  setBasicInfo: PropTypes.func,
  setUser: PropTypes.func,
  setSink: PropTypes.func,
  setResource: PropTypes.func,
  setAlarm: PropTypes.func,
  setEncodes: PropTypes.func,
  setUserParams: PropTypes.func,
  setSinkParams: PropTypes.func,
  setResourceParams: PropTypes.func,
  searchUser: PropTypes.func,
  searchSink: PropTypes.func,
  searchResource: PropTypes.func,
  searchEncode: PropTypes.func,
  getEncodeTypeList: PropTypes.func
}
