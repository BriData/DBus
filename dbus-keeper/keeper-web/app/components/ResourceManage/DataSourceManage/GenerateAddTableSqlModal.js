import React, {Component} from 'react'
import {Button, Form, Modal} from 'antd'
import {DATA_SOURCE_GENERATE_ADD_TABLE_SQL_API} from '@/app/containers/ResourceManage/api'
// 导入样式
import styles from './res/styles/index.less'

@Form.create()
export default class GenerateAddTableSqlModal extends Component {
  constructor(props) {
    super(props)
    this.state = {}
  }

  render() {
    const {visible, key, onClose} = this.props
    return (
      <Modal
        className="top-modal"
        visible={visible}
        maskClosable={false}
        key={key}
        onCancel={onClose}
        width={600}
        title='生成加表脚本'
        footer={[<Button type="primary" onClick={onClose}> 返 回 </Button>]}
      >
        <form id="generateAddTableSql" className={styles.generateAddTableSql}
              action={`${DATA_SOURCE_GENERATE_ADD_TABLE_SQL_API}?token=${window.localStorage.getItem('TOKEN')}`}
              method="post"
              encType="multipart/form-data" target="hidden_frame"
              onChange={() => document.getElementById('generateAddTableSql').submit()}>
          <a className={styles.preProcessA} href='###' onClick={() => {
            document.getElementById('generateAddTableSql').reset()
            document.getElementById('preProcessFile').click()
          }}>
            上传Excel文件
            <input id="preProcessFile" className={styles.preProcessFile} type="file" name="templateFile"/>
          </a>
        </form>
      </Modal>
    )
  }
}

GenerateAddTableSqlModal.propTypes = {}
