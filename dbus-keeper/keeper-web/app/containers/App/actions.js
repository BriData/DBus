import {
  NAVIGATOR_OPEN,
  NAVIGATOR_CLOSE,
  LOAD_NAVSOURCE,
  LOAD_TOP_NAVSOURCE,
  LOAD_BREAD
} from './constants'

export function openNavigator () {
  return {
    type: NAVIGATOR_OPEN
  }
}

export function closeNavigator () {
  return {
    type: NAVIGATOR_CLOSE
  }
}
// 左边菜单
export function loadNavSource (params) {
  return {
    type: LOAD_NAVSOURCE,
    payload: params
  }
}
// 顶部菜单
export function loadTopNavSource (params) {
  return {
    type: LOAD_TOP_NAVSOURCE,
    payload: params
  }
}
// 面包屑菜单
export function setBread (params) {
  return {
    type: LOAD_BREAD,
    payload: params
  }
}
