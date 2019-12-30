import React, {PropTypes, Component} from 'react'
import {Form, Select, Tabs} from 'antd'
const TabPane = Tabs.TabPane;
// 导入组件
import AddDataSource from './AddDataSource'
import AddSchemaTable from './AddSchemaTable'
import AddLogSchemaTable from './AddLogSchemaTable'
import CloneZKTemplate from './CloneZKTemplate'
import StartTopology from './StartTopology'


const FormItem = Form.Item
const Option = Select.Option

export default class DataSourceCreateTabs extends Component {
  constructor(props) {
    super(props)
    this.tabKeyList = [
      'addDs',
      'addSchema',
      'cloneZK',
      'startTopo'
    ]
    this.state = {
      currentStep: 0,
      dataSource: {},
      isShowCloneZk: false
    }
    // this.state = {
    //   isShowCloneZk: true,
    //   currentStep: 2,
    //   dataSource: {
    //     ctrlTopic: "whtest_ctrl",
    //     dbusPwd: "dbus",
    //     dbusUser: "dbus",
    //     dsDesc: "whtest",
    //     dsName: "whtest",
    //     dsType: "oracle",
    //     id: 587,
    //     masterUrl: "jdbc:oracle:thin:@(DESCRIPTION=(FAILOVER = yes)(ADDRESS = (PROTOCOL = TCP)(HOST = vdbus-10)(PORT = 1521))(CONNECT_DATA =(SERVER = DEDICATED)(SERVICE_NAME = orcl)))",
    //     schemaTopic: "whtest_schema",
    //     slaveUrl: "jdbc:oracle:thin:@(DESCRIPTION=(FAILOVER = yes)(ADDRESS = (PROTOCOL = TCP)(HOST = vdbus-10)(PORT = 1521))(CONNECT_DATA =(SERVER = DEDICATED)(SERVICE_NAME = orcl)))",
    //     splitTopic: "whtest_split",
    //     status: "active",
    //     topic: "whtest"
    //   }
    // }

    // this.state = {
    //   currentStep: 1,
    //   dataSource: {
    //     ctrlTopic: "whtest_log_ctrl",
    //     dbusPwd: "empty",
    //     dbusUser: "empty",
    //     dsDesc: "whtest_log",
    //     dsName: "whtest_log",
    //     dsType: "log_logstash",
    //     id: 600,
    //     masterUrl: "empty",
    //     schemaTopic: "whtest_log_schema",
    //     slaveUrl: "empty",
    //     splitTopic: "whtest_log_split",
    //     status: "active",
    //     topic: "whtest_log",
    //   }
    // }

    // this.state = {
    //   isShowCloneZk: true,
    //   currentStep: 2,
    //   dataSource: {
    //     ctrlTopic: "test_dbtwo_ctrl",
    //     dbusPwd: "db2inst",
    //     dbusUser: "db2inst",
    //     dsDesc: "test_dbtwo",
    //     dsName: "test_dbtwo",
    //     dsType: "db2",
    //     id: 628,
    //     masterUrl: "jdbc:db2://10.143.131.162:50001/SAMPLE",
    //     schemaTopic: "test_dbtwo_schema",
    //     slaveUrl: "jdbc:db2://10.143.131.162:50001/SAMPLE",
    //     splitTopic: "test_dbtwo_split",
    //     status: "active",
    //     topic: "test_dbtwo",
    //   }
    // }
  }

  handleAutoCloneZkFail = () => {
    const {currentStep} = this.state
    this.setState({
      ...this.state,
      isShowCloneZk: true,
      currentStep: currentStep + 1
    })
  }

  handleAutoCloneZkSuccess = () => {
    const {currentStep} = this.state
    this.setState({
      ...this.state,
      isShowCloneZk: false,
      currentStep: currentStep + 2
    })
  }

  handleMoveTab = (newState = {}) => {
    const {currentStep} = this.state
    this.setState({
      ...this.state,
      currentStep: currentStep + 1,
      ...newState
    })
  }

  render() {
    const {addDataSourceApi,validateDataSourceApi,addSchemaTableApi} = this.props
    const {dataSource, currentStep, isShowCloneZk} = this.state
    const {schemaList, schemaTableResult} = this.props
    const {getSchemaTableList, getSchemaListByDsId} = this.props
    const {loadZkTreeByDsName, readZkData, saveZkData} = this.props
    const {tree, zkData, cloneConfFromTemplateApi} = this.props
    const {getLatestJarPath} = this.props
    const {jarPath} = this.props
    const {topoJarStartApi} = this.props
    return (
      <div>
        <Tabs activeKey={this.tabKeyList[currentStep]}>
          <TabPane tab="1.添加数据源" key={this.tabKeyList[0]} disabled={currentStep !== 0}>
            <AddDataSource
              validateDataSourceApi={validateDataSourceApi}
              addDataSourceApi={addDataSourceApi}
              onMoveTab={this.handleMoveTab}
            />
          </TabPane>
          <TabPane tab="2.添加Schema和Table" key={this.tabKeyList[1]} disabled={currentStep !== 1}>
            {dataSource.dsType === 'oracle' || dataSource.dsType === 'mysql' || dataSource.dsType === 'mongo'
            || dataSource.dsType === 'db2'
              ? (
              <AddSchemaTable
                addSchemaTableApi={addSchemaTableApi}
                dataSource={dataSource}
                schemaList={schemaList}
                schemaTableResult={schemaTableResult}
                getSchemaListByDsId={getSchemaListByDsId}
                getSchemaTableList={getSchemaTableList}
                onMoveTab={this.handleMoveTab}
                cloneConfFromTemplateApi={cloneConfFromTemplateApi}
                onAutoCloneZkFail={this.handleAutoCloneZkFail}
                onAutoCloneZkSuccess={this.handleAutoCloneZkSuccess}
              />
            ) : (
              <AddLogSchemaTable
                addSchemaTableApi={addSchemaTableApi}
                dataSource={dataSource}
                onMoveTab={this.handleMoveTab}
                cloneConfFromTemplateApi={cloneConfFromTemplateApi}
                onAutoCloneZkFail={this.handleAutoCloneZkFail}
                onAutoCloneZkSuccess={this.handleAutoCloneZkSuccess}
              />
            )}

          </TabPane>
          {isShowCloneZk && (
            <TabPane tab="3.克隆ZK" key={this.tabKeyList[2]} disabled={currentStep !== 2}>
              <CloneZKTemplate
                dataSource={dataSource}
                loadZkTreeByDsName={loadZkTreeByDsName}
                readZkData={readZkData}
                saveZkData={saveZkData}
                tree={tree}
                zkData={zkData}
                cloneConfFromTemplateApi={cloneConfFromTemplateApi}
                onMoveTab={this.handleMoveTab}
              />
            </TabPane>
          )}
          <TabPane tab={`${isShowCloneZk ? 4 : 3}.启动Topology`} key={this.tabKeyList[3]} disabled={currentStep !== 3}>
            <StartTopology
              dataSource={dataSource}
              getLatestJarPath={getLatestJarPath}
              jarPath={jarPath}
              topoJarStartApi={topoJarStartApi}
            />
          </TabPane>
        </Tabs>
      </div>
    )
  }
}

DataSourceCreateTabs.propTypes = {}
