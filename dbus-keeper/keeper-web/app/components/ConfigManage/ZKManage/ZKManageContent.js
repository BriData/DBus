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
    const {zkData} = this.props
    return (
      <div>
        <Tabs defaultActiveKey="nodeData">
          <TabPane tab="Node Data" key="nodeData">
            <Form>
              <FormItem style={{marginBottom: 10}}>
                {getFieldDecorator('value', {
                  initialValue: zkData && zkData.content
                })(
                  <Textarea autosize={{minRows: 25, maxRows: 25}}/>
                )}
              </FormItem>
              <Button onClick={this.handleSave}>Save</Button>
            </Form>
          </TabPane>
          <TabPane tab="Node Meta Data" key="nodeMeta">
            <Form>
              <FormItem
                label='czxid'
                {...formItemLayout}
              >
                {getFieldDecorator('czxid', {
                  initialValue: zkData && zkData.stat && zkData.stat.czxid
                })(
                  <Input readOnly={true}/>
                )}
              </FormItem>
              <FormItem
                label='mzxid'
                {...formItemLayout}
              >
                {getFieldDecorator('mzxid', {
                  initialValue: zkData && zkData.stat && zkData.stat.mzxid
                })(
                  <Input readOnly={true}/>
                )}
              </FormItem>
              <FormItem
                label='ctime'
                {...formItemLayout}
              >
                {getFieldDecorator('ctime', {
                  initialValue: zkData && zkData.stat && zkData.stat.ctime
                })(
                  <Input readOnly={true}/>
                )}
              </FormItem>
              <FormItem
                label='mtime'
                {...formItemLayout}
              >
                {getFieldDecorator('mtime', {
                  initialValue: zkData && zkData.stat && zkData.stat.mtime
                })(
                  <Input readOnly={true}/>
                )}
              </FormItem>
              <FormItem
                label='version'
                {...formItemLayout}
              >
                {getFieldDecorator('version', {
                  initialValue: zkData && zkData.stat && zkData.stat.version
                })(
                  <Input readOnly={true}/>
                )}
              </FormItem>
              <FormItem
                label='cversion'
                {...formItemLayout}
              >
                {getFieldDecorator('cversion', {
                  initialValue: zkData && zkData.stat && zkData.stat.cversion
                })(
                  <Input readOnly={true}/>
                )}
              </FormItem>
              <FormItem
                label='aversion'
                {...formItemLayout}
              >
                {getFieldDecorator('aversion', {
                  initialValue: zkData && zkData.stat && zkData.stat.aversion
                })(
                  <Input readOnly={true}/>
                )}
              </FormItem>
              <FormItem
                label='ephemeralOwner'
                {...formItemLayout}
              >
                {getFieldDecorator('ephemeralOwner', {
                  initialValue: zkData && zkData.stat && zkData.stat.ephemeralOwner
                })(
                  <Input readOnly={true}/>
                )}
              </FormItem>
              <FormItem
                label='dataLength'
                {...formItemLayout}
              >
                {getFieldDecorator('dataLength', {
                  initialValue: zkData && zkData.stat && zkData.stat.dataLength
                })(
                  <Input readOnly={true}/>
                )}
              </FormItem>
              <FormItem
                label='numChildren'
                {...formItemLayout}
              >
                {getFieldDecorator('numChildren', {
                  initialValue: zkData && zkData.stat && zkData.stat.numChildren
                })(
                  <Input readOnly={true}/>
                )}
              </FormItem>
              <FormItem
                label='pzxid'
                {...formItemLayout}
              >
                {getFieldDecorator('pzxid', {
                  initialValue: zkData && zkData.stat && zkData.stat.pzxid
                })(
                  <Input readOnly={true}/>
                )}
              </FormItem>
            </Form>
          </TabPane>
          <TabPane tab="Node ACLs" key="nodeAcl">
            <Form>
              <FormItem
                label='scheme'
                {...formItemLayout}
              >
                {getFieldDecorator('scheme', {
                  initialValue: zkData && zkData.acl && zkData.acl.scheme
                })(
                  <Input readOnly={true}/>
                )}
              </FormItem>
              <FormItem
                label='id'
                {...formItemLayout}
              >
                {getFieldDecorator('id', {
                  initialValue: zkData && zkData.acl && zkData.acl.id
                })(
                  <Input readOnly={true}/>
                )}
              </FormItem>
              <FormItem
                label='permission'
                {...formItemLayout}
              >
                {getFieldDecorator('perms', {
                  initialValue: zkData && zkData.acl && zkData.acl.perms
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
