import React, { Component, PropTypes } from 'react'
import { connect } from 'react-redux'
import { createStructuredSelector } from 'reselect'
import { setBread } from '../App/actions'
import { makeBreadModel } from '@/app/containers/App/selectors'
// 链接reducer和action
@connect(
  createStructuredSelector({
    bread: makeBreadModel()
  }),
  dispatch => ({
    setBread: param => dispatch(setBread(param))
  })
)
export default class Bread extends Component {
  componentWillMount () {
    // 设置面包屑
    const { source, setBread } = this.props
    setBread(source || null)
  }
  render () {
    return <span />
  }
}
Bread.propTypes = {
  source: PropTypes.Array,
  setBread: PropTypes.func
}
