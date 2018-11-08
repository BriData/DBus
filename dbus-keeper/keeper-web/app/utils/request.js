import axios from 'axios'
import { Base64 } from 'js-base64'

const TOKEN_STORAGE = window.localStorage.getItem('TOKEN')

axios.defaults.validateStatus = function (status) {
  return status < 500
}

function getData (response) {
  return response.data
}

function checkStatus (response) {
  if (response && response.data && response.data.status >= 1004 && response.data.status <= 1006) {
    // Message.error('未登录或会话过期，请重新登录')
    removeToken()
    localStorage.removeItem('TOKEN')
    localStorage.removeItem('USERNAME')
    window.location = '/login'
  }
  return response
}

export default function request (url, options) {
  if (TOKEN_STORAGE) setToken(TOKEN_STORAGE)
  return axios(url, options)
    .then(checkStatus)
    .then(getData)
}

export function setToken (token) {
  axios.defaults.headers.common['Authorization'] = token
}

export function removeToken () {
  delete axios.defaults.headers.common['Authorization']
  document.cookie = `token=`
  document.cookie = `TOKEN=`
}

export function getUserInfo () {
  const userBase64 = TOKEN_STORAGE && TOKEN_STORAGE.split('.')[1]
  return userBase64 ? JSON.parse(Base64.decode(userBase64)) : null
}
