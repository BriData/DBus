import { FormattedMessage } from 'react-intl'
import React, {PropTypes, Component} from 'react'
import {Form, Select, Input, Row, Col, Button, Tabs} from 'antd'
const TabPane = Tabs.TabPane;
const Textarea = Input.TextArea
// 导入样式
import styles from './res/styles/index.less'
import Request from "@/app/utils/request";
const FormItem = Form.Item
const Option = Select.Option

@Form.create()
export default class AdvancedConfigForm extends Component {
  constructor(props) {
    super(props)
    this.state = {}
  }

  renderFormItems = config => {
    const itemList = [
      {prop:'heartbeatInterval',hint:'（插入心跳间隔：s）'},
      {prop:'checkInterval',hint:'（心跳超时检查间隔：s）'},
      {prop:'checkFullPullInterval',hint:'（拉取全量检查间隔：s）'},
      {prop:'deleteFullPullOldVersionInterval',hint:'（删除全量时间间隔：h）'},
      {prop:'maxAlarmCnt',hint:'（最大报警次数）'},
      {prop:'heartBeatTimeout',hint:'（心跳超时时间：ms）'},
      {prop:'fullPullTimeout',hint:'（拉取全量超时时间）'},
      {prop:'alarmTtl',hint:'（报警超时时间：ms）'},
      {prop:'lifeInterval',hint:'（心跳生命周期间隔：s）'},
      {prop:'correcteValue',hint:'（心跳不同服务器时间修正值）'},
      {prop:'fullPullCorrecteValue',hint:'（拉取全量不同服务器时间修正值）'},
      {prop:'fullPullSliceMaxPending',hint:'（拉取全量kafka offset无消费最大消息数）'},
      {prop:'leaderPath',hint:'（心跳主备选举控制路径）'},
      {prop:'controlPath',hint:'（心跳控制路径）'},
      {prop:'monitorPath',hint:'（心跳监控路径）'},
      {prop:'monitorFullPullPath',hint:'（拉取全量监控路径）'},
      {prop:'excludeSchema',hint:'（不做监控schema）'},
      {prop:'checkPointPerHeartBeatCnt',hint:'（心跳检查点间隔点数）'},
      {prop:'fullPullOldVersionCnt',hint:'（拉取全量保留版本数）'},
    ]
    const {getFieldDecorator} = this.props.form
    const {onValueChange} = this.props
    const formItemLayout = {
      labelCol: {
        xs: {span: 4},
        sm: {span: 4}
      },
      wrapperCol: {
        xs: {span: 19},
        sm: {span: 18}
      }
    }
    const formContentLayout = {
      gutter: 8,
      input: 12,
      hint: 12
    }

    return itemList.map(item => (
      <FormItem key={item.prop} label={item.prop} {...formItemLayout}>
        <Row gutter={formContentLayout.gutter}>
          <Col span={formContentLayout.input}>
            {getFieldDecorator(item.prop, {
              initialValue: config[item.prop],
              rules: [
                {
                  required: true,
                  message: '不能为空'
                }
              ]
            })
            (<Input onChange={e => onValueChange(e.target.value, item.prop)} placeholder={item.prop} size="large" type="text"/>)}
          </Col>
          <Col span={formContentLayout.hint}>
            {item.hint}
          </Col>
        </Row>
      </FormItem>
    ))
  }

  render() {
    const tailFormItemLayout = {
      wrapperCol: {
        span: 12,
        offset: 4,
      }
    }

    const {config, onSave} = this.props
    return (
      <div>
        <Form autoComplete="off" className="heartbeat-advance-config-form">
          {this.renderFormItems(config)}
          <FormItem {...tailFormItemLayout}>
            <Button type="primary" onClick={onSave}>
              <FormattedMessage
                id="app.common.save"
                defaultMessage="保存"
              />
            </Button>
          </FormItem>
        </Form>
      </div>
    )
  }
}

AdvancedConfigForm.propTypes = {}
