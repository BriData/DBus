/**
 * @author 戎晓伟
 * @description  Sink信息设置
 */

import React, { PropTypes, Component } from 'react'
import { Form, Select, Input, Button, Row, Col } from 'antd'
import { FormattedMessage } from 'react-intl'
import { intlMessage } from '@/app/i18n'
import { fromJS } from 'immutable'
// 导入样式
import styles from './res/styles/index.less'

const FormItem = Form.Item
const Option = Select.Option

@Form.create()
export default class UserManageSearch extends Component {
  /**
   * 校验并查询
   */
  handleSearch = e => {
    const { validateFields } = this.props.form
    const { onSearch, userListParams } = this.props
    // 校验并查询
    validateFields((err, value) => {
      if (!err) {
        const newValue = {
          userName: value.userName || null,
          email: value.email || null,
          phoneNum: value.phoneNum || null
        }
        onSearch({ ...userListParams, ...newValue }, true)
      }
    })
  };
  /**
   * @deprecated input placeholder
   */
  handlePlaceholder = fun => id =>
    fun({
      id: 'app.components.input.placeholder',
      valus: {
        name: fun({ id })
      }
    });
  render () {
    const { locale, onCreateUser } = this.props
    const { getFieldDecorator } = this.props.form
    const localeMessage = intlMessage(locale)
    const placeholder = this.handlePlaceholder(localeMessage)
    return (
      <div className="form-search">
        <Form autoComplete="off" layout="inline" className={styles.searchForm} onKeyUp={e => e.keyCode === 13 && this.handleSearch()}>
          <Row>
            <Col span={4} className={styles.formLeft}>
              <Button
                size="large"
                type="primary"
                icon="plus"
                onClick={() => onCreateUser()}
              >
                <FormattedMessage id="app.common.added" defaultMessage="新增" />
              </Button>
            </Col>
            <Col span={20} className={styles.formRight}>
              <FormItem
                label={
                  <FormattedMessage
                    id="app.common.user.name"
                    defaultMessage="姓名"
                  />
                }
              >
                {getFieldDecorator('userName')(
                  <Input
                    className={styles.input}
                    placeholder={placeholder('app.common.user.name')}
                  />
                )}
              </FormItem>
              <FormItem
                label={
                  <FormattedMessage
                    id="app.common.user.email"
                    defaultMessage="邮箱"
                  />
                }
              >
                {getFieldDecorator('email')(
                  <Input
                    className={styles.input}
                    placeholder={placeholder('app.common.user.email')}
                  />
                )}
              </FormItem>
              <FormItem
                label={
                  <FormattedMessage
                    id="app.common.user.phone"
                    defaultMessage="手机号"
                  />
                }
              >
                {getFieldDecorator('phoneNum')(
                  <Input
                    className={styles.input}
                    placeholder={placeholder('app.common.user.phone')}
                  />
                )}
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
            </Col>
          </Row>
        </Form>
      </div>
    )
  }
}

UserManageSearch.propTypes = {
  locale: PropTypes.any,
  form: PropTypes.object,
  userListParams: PropTypes.object,
  onSearch: PropTypes.func,
  onCreateUser: PropTypes.func
}
