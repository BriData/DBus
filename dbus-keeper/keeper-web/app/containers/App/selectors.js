import { createSelector } from 'reselect'

const selectGlobal = (state) => state.get('global')

const makeSelectNavCollapsed = () => createSelector(
  selectGlobal,
  (globalState) => globalState.get('navCollapsed')
)

const makeSelectNavSource = () => createSelector(
  selectGlobal,
  (globalState) => globalState.get('leftMenuSource')
)

const makeSelectLocationState = () => {
  let prevRoutingState
  let prevRoutingStateJS

  return (state) => {
    const routingState = state.get('route') // or state.route

    if (!routingState.equals(prevRoutingState)) {
      prevRoutingState = routingState
      prevRoutingStateJS = routingState.toJS()
    }

    return prevRoutingStateJS
  }
}
const makeSelectTopNavSource = () => createSelector(
  selectGlobal,
  (globalState) => globalState.get('topMenuSource')
)
// 面包屑
const makeBreadModel = () => createSelector(
  selectGlobal,
  (globalState) => globalState.get('bread')
)
export {
  selectGlobal,
  makeSelectNavCollapsed,
  makeSelectNavSource,
  makeSelectTopNavSource,
  makeSelectLocationState,
  makeBreadModel
}
