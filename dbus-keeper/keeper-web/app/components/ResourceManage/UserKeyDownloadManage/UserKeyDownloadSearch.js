import React, { PropTypes, Component } from 'react'
import { Form, Select, Input, Button, Row, Col } from 'antd'
// 导入样式
import styles from './res/styles/index.less'

const FormItem = Form.Item
const Option = Select.Option

export default class UserKeyDownloadSearch extends Component {
  constructor (props) {
    super(props)
  }
  componentWillMount () {
  }

  render () {
    const {onOpenDownload} = this.props
    return (
      <div className="form-search">
        <Form autoComplete="off" layout="inline" className={styles.searchForm}>
          <Row>
            <Col span={4} className={styles.formLeft}>
              <FormItem>
                <Button
                  type="primary"
                  onClick={onOpenDownload}
                >
                  {"下载密钥文件"}
                </Button>
              </FormItem>
            </Col>
          </Row>
        </Form>
      </div>
    )
  }
}

UserKeyDownloadSearch.propTypes = {
}
