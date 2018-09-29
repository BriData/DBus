import React, {PropTypes, Component} from 'react'
import {Modal, Form, Select, Input, Row, Col, Button, message, Table} from 'antd'
import {FormattedMessage} from 'react-intl'

// 导入样式
import styles from './res/styles/index.less'

const keyList = ["column_name", "data_type", "data_length", "data_scale", "comments"]
const len = keyList.length

export default class DataTableManageVersionCompareTable extends Component {
  constructor(props) {
    super(props)
  }

  createContent = (data) => {
    let content = []
    if (!data) return null

    data.map(function (row) {
      const v1TdList = [], v2TdList = []
      let v1 = row["v1"]
      let v2 = row["v2"]
      let rowContent = []
      if (!v1) {
        let tdList = []
        for (let i = 0; i < len - 1; i++) {
          tdList.push(<td/>)
        }
        tdList.push(<td className={styles.borderTd}/>)
        for (let i = 0; i < len; i++) {
          tdList.push(<td key={i}>{v2[keyList[i]]}</td>)
        }
        rowContent.push(<tr className={styles.danger}>{tdList}</tr>)
      } else if (!v2) {
        let tdList = []
        for (let i = 0; i < len - 1; i++) {
          tdList.push(<td>{v1[keyList[i]]}</td>)
        }
        tdList.push(<td className={styles.borderTd}>{v1[keyList[len - 1]]}</td>)
        for (let i = 0; i < len; i++) {
          tdList.push(<td/>)
        }
        rowContent.push(<tr className={styles.danger}>{tdList}</tr>)
      } else {
        let isRowDifferent = false
        keyList.map(function (key) {
          if (v1[key] !== v2[key]) {
            if (key === keyList[len - 1]) {
              v1TdList.push(<td className={{...styles.borderTd, ...styles.danger}}>{v1[key]}</td>)
            } else {
              v1TdList.push(<td className={styles.danger}>{v1[key]}</td>)
            }
            v2TdList.push(<td className={styles.danger}>{v2[key]}</td>)
            isRowDifferent = true
          } else {
            if (key === keyList[len - 1]) {
              v1TdList.push(<td className={styles.borderTd}>{v1[key]}</td>)
            } else {
              v1TdList.push(<td>{v1[key]}</td>)
            }

            v2TdList.push(<td>{v2[key]}</td>)
          }
        })
        if (isRowDifferent) {
          rowContent.push(<tr className={styles.warning}>{v1TdList}{v2TdList}</tr>)
        } else {
          rowContent.push(<tr>{v1TdList}{v2TdList}</tr>)
        }

      }
      content.push(rowContent)
    })
    return content
  }

  render = () => {
    const {content} = this.props
    return (
      <div style={{
        maxHeight: 420,
        overflowX: "auto",
        overflowY: "auto",
        width: "100%"
      }}>
        <table className={styles.versionCompareTable} cellPadding={0} cellSpacing={0}>
          <thead>
          <tr>
            <th >
              Name
            </th>
            <th>
              Type
            </th>
            <th>
              Length
            </th>
            <th>
              Scale
            </th>
            <th style={{borderRight: "#bbbbbb solid 1px"}}>
              comments
            </th>
            <th>
              Name
            </th>
            <th>
              Type
            </th>
            <th>
              Length
            </th>
            <th>
              Scale
            </th>
            <th>
              comments
            </th>
          </tr>
          </thead>
          <tbody>
          {this.createContent(content)}
          </tbody>
        </table>
      </div>
    )
  }
}

DataTableManageVersionCompareTable.propTypes = {}
