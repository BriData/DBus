import {fromJS} from 'immutable'
import {combineReducers} from 'redux-immutable'
import {LOCATION_CHANGE} from 'react-router-redux'

import globalReducer from 'containers/App/reducer'
import languageProviderReducer from 'containers/LanguageProvider/reducer'
// 导入reducer
import {
  projectFullpullReducer,
  projectHomeReducer,
  projectResourceReducer,
  projectSummaryReducer,
  projectTableReducer,
  projectTopologyReducer
} from 'containers/ProjectManage/redux'

import {sinkerSchemaReducer, sinkerTableReducer, sinkHomeReducer} from 'containers/SinkManage/redux'

import {userManageReducer} from 'containers/UserManage/redux'

import {
  DataSchemaReducer,
  DataSourceCreateReducer,
  DataSourceReducer,
  DataTableReducer,
  DBusDataReducer,
  EncodeManagerReducer,
  EncodePluginManageReducer,
  JarManageReducer,
  RuleGroupReducer
} from 'containers/ResourceManage/redux'

import {ControlMessageReducer, GlobalFullpullReducer, KafkaReaderReducer} from 'containers/toolSet/redux'

import {GlobalConfigReducer, ZKManageReducer} from 'containers/ConfigManage/redux'

const routeInitialState = fromJS({
  locationBeforeTransitions: null
})

function routeReducer(state = routeInitialState, action) {
  switch (action.type) {
    /* istanbul ignore next */
    case LOCATION_CHANGE:
      return state.merge({
        locationBeforeTransitions: action.payload
      })
    default:
      return state
  }
}

export default function createReducer(asyncReducers) {
  return combineReducers({
    route: routeReducer,
    global: globalReducer,
    language: languageProviderReducer,
    projectHomeReducer,
    projectResourceReducer,
    projectTopologyReducer,
    projectSummaryReducer,
    projectTableReducer,
    projectFullpullReducer,
    sinkHomeReducer,
    sinkerSchemaReducer,
    sinkerTableReducer,
    userManageReducer,
    EncodePluginManageReducer,
    JarManageReducer,
    DataSourceReducer,
    DataSchemaReducer,
    DataTableReducer,
    DBusDataReducer,
    EncodeManagerReducer,
    RuleGroupReducer,
    DataSourceCreateReducer,
    ControlMessageReducer,
    GlobalFullpullReducer,
    KafkaReaderReducer,
    ZKManageReducer,
    GlobalConfigReducer,
    ...asyncReducers
  })
}
