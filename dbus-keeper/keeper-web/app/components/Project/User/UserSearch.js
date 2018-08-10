/**
 * @author 戎晓伟
 * @description  Sink信息设置
 */

import React, { PropTypes, Component } from 'react'
import { Form, Button, Row, Col } from 'antd'
import { FormattedMessage } from 'react-intl'
// 导入样式
import styles from './res/styles/index.less'

export default class UserSearch extends Component {
  render () {
    const { onModifyUser, projectId } = this.props
    return (
      <div className="form-search">
        <Form autoComplete="off" layout="inline" className={styles.searchForm}>
          <Row>
            {/* <Col span={2} className={styles.formLeft}>
              <Button
                size="large"
                type="primary"
                icon="edit"
                onClick={() => onModifyUser(projectId)}
                className={styles.button}
                >
                调整项目用户
              </Button>
            </Col>
            <Col span={22} className={styles.formRight}>
              <FormItem>
                {getFieldDecorator('userName', {
                  initialValue: null
                })(<Input className={styles.input} placeholder="userName" />)}
              </FormItem>
              <FormItem>
                {getFieldDecorator('email', {
                  initialValue: null
                })(<Input className={styles.input} placeholder="email" />)}
              </FormItem>
              <FormItem>
                {getFieldDecorator('phoneNum', {
                  initialValue: null
                })(<Input className={styles.input} placeholder="phoneNum" />)}
              </FormItem>
              <FormItem>
                <Button
                  type="primary"
                  icon="search"
                  onClick={this.handleSearch}
                >
                  <FormattedMessage
                    id="app.common.search"
                    defaultMessage="查询"
                  />
                </Button>
              </FormItem>
            </Col> */}
            <Col span={24} className={styles.formLeft}>
              <Button
                size="large"
                type="primary"
                icon="edit"
                onClick={() => onModifyUser(projectId)}
                className={styles.button}
              >
                调整项目用户
              </Button>
            </Col>
          </Row>
        </Form>
      </div>
    )
  }
}

UserSearch.propTypes = {
  onModifyUser: PropTypes.func
}
