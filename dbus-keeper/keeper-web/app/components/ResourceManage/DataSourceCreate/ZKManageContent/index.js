import React, { PropTypes, Component } from 'react'
import { Form, Select, Input, message,Table, Button,Tabs } from 'antd'
import { FormattedMessage } from 'react-intl'
import OperatingButton from '@/app/components/common/OperatingButton'
const TabPane = Tabs.TabPane;
const Textarea = Input.TextArea
// 导入样式
import styles from './res/styles/index.less'
import Request from "@/app/utils/request";

const FormItem = Form.Item
const Option = Select.Option

@Form.create()
export default class ZKManageContent extends Component {
  constructor (props) {
    super(props)
    this.state = {

    }
  }

  handleSave = () => {
    const {path, onSave} = this.props
    if (!path) {
      message.error('请选择节点')
      return
    }
    this.props.form.validateFieldsAndScroll((err, values) => {
      if (!err) {
        console.info(path, values)
        onSave(path, values.value)
      }
    })
  }

  render () {
    const { getFieldDecorator } = this.props.form
    const formItemLayout = {
      labelCol: {
        xs: { span: 3 },
        sm: { span: 3 }
      },
      wrapperCol: {
        xs: { span: 19 },
        sm: { span: 12 }
      }
    }
    const {content} = this.props
    return (
      <div>
        <Tabs defaultActiveKey="nodeData">
          <TabPane tab="Node Data" key="nodeData">
            <Form>
              <FormItem style={{marginBottom: 10}}>
                {getFieldDecorator('value', {
                  initialValue: content ? content : ''
                })(
                  <Textarea autosize={{minRows: 25, maxRows: 25}}/>
                )}
              </FormItem>
              <Button onClick={this.handleSave}>Save</Button>
            </Form>
          </TabPane>
          <TabPane disabled tab="Node Meta Data" key="nodeMeta">
            <Form>
              <FormItem
                label='czxid'
                {...formItemLayout}
              >
                {getFieldDecorator('czxid', {
                  initialValue: 'czxid'
                })(
                  <Input readOnly={true}/>
                )}
              </FormItem>
              <FormItem
                label='mzxid'
                {...formItemLayout}
              >
                {getFieldDecorator('mzxid', {
                  initialValue: 'mzxid'
                })(
                  <Input readOnly={true}/>
                )}
              </FormItem>
              <FormItem
                label='ctime'
                {...formItemLayout}
              >
                {getFieldDecorator('ctime', {
                  initialValue: 'ctime'
                })(
                  <Input readOnly={true}/>
                )}
              </FormItem>
              <FormItem
                label='czxid'
                {...formItemLayout}
              >
                {getFieldDecorator('czxid', {
                  initialValue: 'czxid'
                })(
                  <Input readOnly={true}/>
                )}
              </FormItem>
              <FormItem
                label='mtime'
                {...formItemLayout}
              >
                {getFieldDecorator('mtime', {
                  initialValue: 'mtime'
                })(
                  <Input readOnly={true}/>
                )}
              </FormItem>
              <FormItem
                label='version'
                {...formItemLayout}
              >
                {getFieldDecorator('version', {
                  initialValue: 'version'
                })(
                  <Input readOnly={true}/>
                )}
              </FormItem>
              <FormItem
                label='cversion'
                {...formItemLayout}
              >
                {getFieldDecorator('cversion', {
                  initialValue: 'cversion'
                })(
                  <Input readOnly={true}/>
                )}
              </FormItem>
              <FormItem
                label='aversion'
                {...formItemLayout}
              >
                {getFieldDecorator('aversion', {
                  initialValue: 'aversion'
                })(
                  <Input readOnly={true}/>
                )}
              </FormItem>
              <FormItem
                label='ephemeralOwner'
                {...formItemLayout}
              >
                {getFieldDecorator('ephemeralOwner', {
                  initialValue: 'ephemeralOwner'
                })(
                  <Input readOnly={true}/>
                )}
              </FormItem>
              <FormItem
                label='dataLength'
                {...formItemLayout}
              >
                {getFieldDecorator('dataLength', {
                  initialValue: 'dataLength'
                })(
                  <Input readOnly={true}/>
                )}
              </FormItem>
              <FormItem
                label='numChildren'
                {...formItemLayout}
              >
                {getFieldDecorator('numChildren', {
                  initialValue: 'numChildren'
                })(
                  <Input readOnly={true}/>
                )}
              </FormItem>
              <FormItem
                label='pzxid'
                {...formItemLayout}
              >
                {getFieldDecorator('pzxid', {
                  initialValue: 'pzxid'
                })(
                  <Input readOnly={true}/>
                )}
              </FormItem>
            </Form>
          </TabPane>
          <TabPane disabled tab="Node ACLs" key="nodeAcl">
            <Form>
              <FormItem
                label='scheme'
                {...formItemLayout}
              >
                {getFieldDecorator('scheme', {
                  initialValue: 'scheme'
                })(
                  <Input readOnly={true}/>
                )}
              </FormItem>
              <FormItem
                label='id'
                {...formItemLayout}
              >
                {getFieldDecorator('id', {
                  initialValue: 'id'
                })(
                  <Input readOnly={true}/>
                )}
              </FormItem>
              <FormItem
                label='permission'
                {...formItemLayout}
              >
                {getFieldDecorator('permission', {
                  initialValue: 'permission'
                })(
                  <Input readOnly={true}/>
                )}
              </FormItem>
            </Form>
          </TabPane>
        </Tabs>
      </div>
    )
  }
}

ZKManageContent.propTypes = {
}
