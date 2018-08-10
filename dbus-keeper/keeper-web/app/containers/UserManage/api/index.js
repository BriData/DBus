/**
 * @author 戎晓伟
 * @description API
 */

// 查询用户列表
// params {pageSize,pageNum}
export const SEARCH_USER_LIST_API = '/keeper/users/search'

// 获取用户信息
// params  id
export const GET_USER_INFO_API = '/keeper/users'

// 创建用户信息
// params  {}
export const CREATE_USER_API = '/keeper/users/create'

// 修改用户信息
// params id {}
export const UPDATE_USER_API = '/keeper/users/update'

// 删除用户
// params id {}
export const DELETE_USER_API = '/keeper/users/delete'

// 查询该用户已分配的项目

// params {userId,roleType}
export const GET_USER_PROJECT_API = '/keeper/projects'

// 用户自己修改密码
export const USER_CHANGE_PASSWORD_API = '/keeper/users/modifyPassword'
