/**
 * @author 戎晓伟
 * @description saga
 */

import { message } from 'antd'
import { call, put } from 'redux-saga/effects'
import { takeLatest } from 'redux-saga'
import Request from '@/app/utils/request'

// 导入API
import {
} from '@/app/containers/ConfigManage/api'

// 导入 action types
import {
} from '../redux/action/types'

// 导入 action
import {
} from '../redux/action'

function* GlobalConfigManage () {
  yield [
  ]
}

// All sagas to be loaded
export default [GlobalConfigManage]
