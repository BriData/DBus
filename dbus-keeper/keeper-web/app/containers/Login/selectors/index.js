import { createSelector } from 'reselect'

// 获取list state
const LoginModel = () => createSelector(
  (state) => state.get('loginReducer'),
  (state) => state.toJS()
)
export {
  LoginModel
}
