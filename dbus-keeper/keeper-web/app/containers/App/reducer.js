import {
  NAVIGATOR_OPEN,
  NAVIGATOR_CLOSE,
  LOAD_NAVSOURCE,
  LOAD_TOP_NAVSOURCE,
  LOAD_BREAD
} from './constants'
import { fromJS } from 'immutable'

const initialState = fromJS({
  navCollapsed: false,
  topMenuSource: null,
  leftMenuSource: null,
  bread: null
})

function appReducer (state = initialState, { type, payload }) {
  switch (type) {
    case NAVIGATOR_OPEN:
      return state.set('navCollapsed', false)
    case NAVIGATOR_CLOSE:
      return state.set('navCollapsed', true)
    case LOAD_NAVSOURCE:
      return state.set('leftMenuSource', payload)
    case LOAD_TOP_NAVSOURCE:
      return state.set('topMenuSource', payload)
    case LOAD_BREAD:
      return state.set('bread', payload)
    default:
      return state
  }
}

export default appReducer
