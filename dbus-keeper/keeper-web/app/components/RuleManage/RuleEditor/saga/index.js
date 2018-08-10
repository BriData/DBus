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
  SEARCH_RULE_GROUP_API
} from '@/app/containers/ResourceManage/api'

// 导入 action types
import {
  SEARCH_RULE_GROUP
} from '../redux/action/types'

// 导入 action
import { searchRuleGroup } from '../redux/action'

// 查询jar包信息
function* searchRuleGroupRepos (action) {
  const requestUrl = SEARCH_RULE_GROUP_API
  try {
    const repos = yield call(Request, `${requestUrl}/${action.result.tableId}`, {
      method: 'get'
    })
    yield put(
      searchRuleGroup.success((repos.status === 0 && repos.payload) || null)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(searchRuleGroup.fail(err))
  }
}

function* RuleGroupManage () {
  yield [
    yield takeLatest(SEARCH_RULE_GROUP.LOAD, searchRuleGroupRepos),
  ]
}

// All sagas to be loaded
export default [RuleGroupManage]
