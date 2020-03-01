import React, {Component} from 'react'
import {Button, Col, Form, Popconfirm, Row} from 'antd'
// 导入样式
import styles from './res/styles/index.less'

const FormItem = Form.Item

@Form.create()
export default class OggCanalDeploySearch extends Component {
  constructor(props) {
    super(props)
  }

  componentWillMount() {
  }


  render() {
    const {getFieldDecorator} = this.props.form
    const {loading, onSync} = this.props
    return (
      <div className="form-search">
        <Form autoComplete="off"
              layout="inline"
              className={styles.searchForm}
              onKeyUp={e => e.keyCode === 13 && this.handleSearch()}
        >
          <Row>
            <Col span={24} className={styles.formRight}>
              <FormItem>
                <Popconfirm title={'确认更新部署详情？'} onConfirm={() => onSync()} okText="Yes" cancelText="No">
                  <Button
                    type="primary"
                    icon="sync"
                    loading={loading}
                  >
                    更新部署详情
                  </Button>
                </Popconfirm>
              </FormItem>
            </Col>
          </Row>
        </Form>
      </div>
    )
  }
}

OggCanalDeploySearch.propTypes = {}
