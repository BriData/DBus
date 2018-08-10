/**
 * @author 戎晓伟
 * @description redux->reducer
 */

import { fromJS } from 'immutable'

// 导入types
import {
} from '../action/types'

const initialState = fromJS({
})

export default (state = initialState, action) => {
  switch (action.type) {
    default:
      return state
  }
}
