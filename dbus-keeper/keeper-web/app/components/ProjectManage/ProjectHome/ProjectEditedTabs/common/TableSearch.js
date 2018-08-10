/**
 * @author 戎晓伟
 * @param formItems [object Array] [{label,key}] [{展示的文字,存储的key}]
 * @param onSearch  [object Function] 查询方法
 * @param children  [react Element] 附加的子组件
 * @description  Table的普通查询公共组件
 */

import React, { PropTypes, Component } from 'react'
import { Form, Button, Input } from 'antd'

const FormItem = Form.Item

@Form.create()
export default class TableSearch extends Component {
  // 查询
  handleSearch=() => {
    const getFieldsValue = this.props.form.getFieldsValue()
    this.props.onSearch(getFieldsValue)
  }
  render () {
    const { getFieldDecorator } = this.props.form
    const { onSearch, children, formItems } = this.props
    return (
      <div>
        {formItems ? (
          <Form autoComplete="off" layout="inline">
            {formItems.map(item => (
              <FormItem label={item.label} key={item.key}>
                {getFieldDecorator(item.key, {
                  initialValue: ''
                })(<Input type="text" />)}
              </FormItem>
            ))}
            <FormItem>
              <Button type="primary" icon="search" onClick={onSearch}>
                 查询
              </Button>
            </FormItem>
            <FormItem>{children}</FormItem>
          </Form>
        ) : (
          ''
        )}
      </div>
    )
  }
}

TableSearch.propTypes = {
  form: PropTypes.object,
  formItems: PropTypes.array,
  children: PropTypes.element,
  onSearch: PropTypes.fun
}
