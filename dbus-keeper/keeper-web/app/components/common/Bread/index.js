import React from 'react'
import PropTypes from 'prop-types'
import { Link } from 'react-router'
import { Icon, Breadcrumb } from 'antd'
import styles from './Bread.less'

export function Bread (props) {
  const breadcrumbItems = props.source && props.source.map((s, index, source) => {
    const key = upperCaseFirstLetter(s.name)
    return (
      <Breadcrumb.Item key={key}>
        {source.length - 1 === index && index !== 0 ? (
          <span>{key}</span>
        ) : (
          <a href={s.path || ''}>
            {index === 0 ? <Icon type="home" /> : key}
          </a>
        )}
      </Breadcrumb.Item>
    )
  })
  return (
    <div className={`${props.className} ${styles.bread}`}>
      <Breadcrumb>{breadcrumbItems}</Breadcrumb>
    </div>
  )
}

function upperCaseFirstLetter (str) {
  return str
    ? str
        .split('')
        .reduce(
          (newStr, val) => `${newStr}${newStr ? val : val.toUpperCase()}`,
          ''
        )
    : str
}

Bread.propTypes = {
  source: PropTypes.array,
  className: PropTypes.string
}

export default Bread
