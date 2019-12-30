import { fromJS } from 'immutable'
import { combineReducers } from 'redux-immutable'
import { LOCATION_CHANGE } from 'react-router-redux'

import globalReducer from 'containers/App/reducer'
import languageProviderReducer from 'containers/LanguageProvider/reducer'

// 导入reducer
import {
  projectHomeReducer,
  projectResourceReducer,
  projectTopologyReducer,
  projectSummaryReducer,
  projectTableReducer,
  projectFullpullReducer
} from 'containers/ProjectManage/redux'

import { sinkHomeReducer } from 'containers/SinkManage/redux'

import { userManageReducer } from 'containers/UserManage/redux'

import {
  EncodePluginManageReducer,
  JarManageReducer,
  DataSourceReducer,
  DataSchemaReducer,
  DataTableReducer,
  DBusDataReducer,
  EncodeManagerReducer,
  RuleGroupReducer,
  DataSourceCreateReducer
} from 'containers/ResourceManage/redux'

import {
  ControlMessageReducer,
  GlobalFullpullReducer,
  KafkaReaderReducer,
  BatchRestartTopoReducer
} from 'containers/toolSet/redux'

import {GlobalConfigReducer, ZKManageReducer} from 'containers/ConfigManage/redux'

const routeInitialState = fromJS({
  locationBeforeTransitions: null
})

function routeReducer (state = routeInitialState, action) {
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

export default function createReducer (asyncReducers) {
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
