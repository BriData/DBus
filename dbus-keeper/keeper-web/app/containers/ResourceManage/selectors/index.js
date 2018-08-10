import { createSelector } from 'reselect'

// 获取 encode plugin Manage state
const EncodePluginManageModel = () => createSelector(
  (state) => state.get('EncodePluginManageReducer'),
  (state) => state.toJS()
)

// 获取 JarManage state
const JarManageModel = () => createSelector(
  (state) => state.get('JarManageReducer'),
  (state) => state.toJS()
)
// 获取 DataSource state
const DataSourceModel = () => createSelector(
  (state) => state.get('DataSourceReducer'),
  (state) => state.toJS()
)
// 获取 DataSchema state
const DataSchemaModel = () => createSelector(
  (state) => state.get('DataSchemaReducer'),
  (state) => state.toJS()
)

// 获取 DataTable state
const DataTableModel = () => createSelector(
  (state) => state.get('DataTableReducer'),
  (state) => state.toJS()
)

// 获取 DBusData state
const DBusDataModel = () => createSelector(
  (state) => state.get('DBusDataReducer'),
  (state) => state.toJS()
)

// 获取 RuleGroup state
const RuleGroupModel = () => createSelector(
  (state) => state.get('RuleGroupReducer'),
  (state) => state.toJS()
)

// 获取 datasource create
const DataSourceCreateModel = () => createSelector(
  (state) => state.get('DataSourceCreateReducer'),
  (state) => state.toJS()
)


// 获取 encode manager create
const EncodeManagerModel = () => createSelector(
  (state) => state.get('EncodeManagerReducer'),
  (state) => state.toJS()
)


export {
  DataSourceCreateModel,
  EncodePluginManageModel,
  JarManageModel,
  DataSourceModel,
  DataSchemaModel,
  DataTableModel,
  RuleGroupModel,
  DBusDataModel,
  EncodeManagerModel
}
