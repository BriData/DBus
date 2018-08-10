import React from 'react'

import Icon from 'antd/lib/icon'

import styles from './NotFound.less'

export default function NotFound () {
  return (
    <div className={styles.notFound}>
      <Icon type="frown-o" />
      <h1>您来到了没有数据的地方</h1>
    </div>
  )
}
