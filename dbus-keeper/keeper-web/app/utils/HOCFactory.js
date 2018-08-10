import React, { Component } from 'react'

export default function HOCFactory (params) {
  // do something with params
  return function HOCFactoryFun (WrappedComponent) {
    return class HOC extends Component {
      render () {
        const props = {...this.props, ...params}
        return <WrappedComponent {...props} />
      }
    }
  }
}
