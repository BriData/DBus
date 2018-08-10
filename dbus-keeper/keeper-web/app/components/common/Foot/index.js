import React, { PropTypes } from 'react'

import styles from './Foot.less'

export default function Foot (props) {
  const source = [
    {
      text: '帮助',
      link: '../'
    },
    {
      text: '隐私',
      link: '../'
    },
    {
      text: '条款',
      link: '../'
    }
  ]
  return (
    <div className={`${props.className} ${styles.footer}`}>
      <div className={styles.nav}>
        {
          source.map((item, index) => (<span key={`foot-${index}`}>
            {index !== 0 && <span className="ant-divider" />}
            <a href={item.link} target="_blank">{item.text}</a>
          </span>))
        }
      </div>
      {
        props.copyright && <div className={styles.copyright}>
            {`copyright© ${props.copyright}`}
        </div>
      }
    </div>
  )
}

Foot.propTypes = {
  className: PropTypes.string,
  source: PropTypes.array,
  copyright: PropTypes.string
}

