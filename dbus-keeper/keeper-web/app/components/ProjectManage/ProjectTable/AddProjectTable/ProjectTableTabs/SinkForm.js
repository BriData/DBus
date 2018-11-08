/**
 * @author 戎晓伟
 * @description  基本信息设置
 */

import React, { PropTypes, Component } from 'react'
import { Form, Input, Checkbox, Radio, Select, Row, Col } from 'antd'
import { FormattedMessage } from 'react-intl'
import { intlMessage } from '@/app/i18n'


const FormItem = Form.Item
const RadioGroup = Radio.Group
const Option = Select.Option

@Form.create({ warppedComponentRef: true })
export default class SinkForm extends Component {
  constructor (props) {
    super(props)
    this.formMessage = {
      en: {
        descriptionPlaceholder: 'description,Up to 150 words',
        projectNameMessage: 'The project name is required',
        principalMessage: 'The principal is required',
        topologyMessage:
          'The number of topology must be integers and 0<topologyNum<=100.'
      },
      zh: {
        descriptionPlaceholder: '项目描述，最多150字符',
        projectNameMessage: '项目名称为必填项',
        principalMessage: '负责人为必填项',
        topologyMessage: 'topology 个数必须为整数且 0<topologyNum<=100'
      }
    }
  }

  componentDidMount = () => {
    const { projectId } = this.props
    this.handleSaveToReudx({ projectId })
    const {onGetTableTopics} = this.props
    onGetTableTopics({projectId})
  };
  /**
   * @deprecated radio change 存储数据
   */
  handleRadioChange = e => {
    const value = e.target.value
    // 存储数据
    this.handleSaveToReudx({ outputType: value })
  };
  /**
   * @param value [object String] 存储的值
   * @param key [object String] 存储的key
   * @description select onchange
   */
  handleSelectChange = (value, key) => {
    // 存储数据
    this.handleSaveToReudx({ [key]: value })
  };
  /**
   * @param value [object String] 存储的值
   * @param key [object String] 存储的key
   * @description select onBlur
   */
  handleSelectBlur = (value, key) => {
    // 存储数据
    this.handleSaveToReudx({ [key]: value })
  };
  /**
   * @deprecated sink change 存储数据
   */
  handleSinkChange = (value, key) => {
    this.handleSelectChange(value, key)
  };
  /**
   * @param value [object object] 需要存储的键值对
   * @description 将sink数据存储到redux中
   */
  handleSaveToReudx = value => {
    const { getFieldsValue } = this.props.form
    const { sink, onSetSink } = this.props
    // 设置sink
    onSetSink({ ...sink, ...getFieldsValue(), ...value })
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
    const { sink, sinkList, topicList } = this.props
    const { getFieldDecorator } = this.props.form
    const sinkListArray = Object.values(sinkList.result) || []

    const sinkId = sink && sink.sinkId

    let topicListArray = topicList.result || {}
    topicListArray = sinkId && topicListArray[sinkId] ? topicListArray[sinkId] : []

    const localeMessage = intlMessage(this.props.locale, this.formMessage)
    const placeholder = this.handlePlaceholder(localeMessage)

    let {projectInfo} = this.props
    projectInfo = projectInfo.result.project || {}

    let outputTopic = sink && sink.outputTopic
    if (outputTopic && projectInfo && projectInfo.projectName) {
      if (outputTopic.indexOf(projectInfo.projectName + '.') === 0)
        outputTopic = sink.outputTopic.substr(projectInfo.projectName.length + 1)
    }

    topicListArray = topicListArray.map(topic => {
      if (projectInfo && projectInfo.projectName && topic.indexOf(projectInfo.projectName + '.') === 0) {
        return topic.substr(projectInfo.projectName.length + 1)
      } else {
        return topic
      }
    })

    const formItemLayout = {
      labelCol: { span: 4 },
      wrapperCol: { span: 12 }
    }

    return (
      <Form autoComplete="off" layout="horizontal">
        <FormItem label={<FormattedMessage
          id="app.components.projectManage.projectTable.dataOutputFormat"
          defaultMessage="数据输出格式"
        />} {...formItemLayout}>
          <RadioGroup
            defaultValue={sink && ((sink.outputType === 'json') ? 'json' : 'ums1.3')}
            onChange={this.handleRadioChange}
          >
            <Radio value="ums1.3">ums</Radio>
            <Radio disabled={true} value="json">json</Radio>
          </RadioGroup>
        </FormItem>
        {sink && sink.outputType &&
          sink.outputType !== 'json' && (
            <FormItem label={<FormattedMessage
              id="app.components.projectManage.projectTable.umsOutputVersion"
              defaultMessage="UMS输出版本"
            />} {...formItemLayout}>
              {getFieldDecorator('outputType', {
                initialValue: sink.outputType && 'ums1.3',
                rules: [
                  {
                    required: true,
                    message: localeMessage({ id: 'projectNameMessage' }),
                    whitespace: true
                  }
                ]
              })(
                <Select
                  showSearch
                  optionFilterProp='children'
                  placeholder="Please select outputType"
                  onChange={value => this.handleSelectChange(value, 'outputType')}
                >
                  <Option value="ums1.1">ums 1.1</Option>
                  <Option value="ums1.2">ums 1.2</Option>
                  <Option value="ums1.3">ums 1.3</Option>
                </Select>
              )}
            </FormItem>
          )}
        <FormItem label="Sink" {...formItemLayout}>
          {getFieldDecorator('sinkId', {
            initialValue:
              (sink && `${sink.sinkId}`) || `${sinkListArray && sinkListArray[0].id}`
          })(
            <Select
              showSearch
              optionFilterProp='children'
              placeholder="Please select sinkId"
              onChange={value => this.handleSinkChange(value, 'sinkId')}
            >
              {sinkListArray.length > 0 &&
                sinkListArray.map((item, index) => (
                  <Option value={`${item.id}`} key={`${item.id}`}>
                    {item.sinkName}
                  </Option>
                ))}
            </Select>
          )}
        </FormItem>
        <FormItem label="Topic" {...formItemLayout}>
          <span
            style={{
              width: "auto",
              border: "1px solid rgb(217, 217, 217)",
              float: "left",
              display: "block",
              borderRadius: "4px 0 0 4px",
              boxShadow: "none",
              padding: "9px 5px",
            }}
            className="ant-input-group-addon">
            {projectInfo.projectName + '.'}
          </span>
          {getFieldDecorator('outputTopic', {
            initialValue:
            outputTopic ||
            (topicListArray && topicListArray[0])
          })(
            <Select
              className="sinkTopicSelect"
              style={{
                overflow: "hidden",
                display: "block",
                width: "auto",
              }}
              mode="combobox"
              showSearch
              optionFilterProp="children"
              filterOption={(input, option) =>
                option.props.children
                  .toLowerCase()
                  .indexOf(input.toLowerCase()) >= 0
              }
              placeholder="Please select topic"
              onChange={value => this.handleSelectBlur(value, 'outputTopic')}
            >
              {topicListArray.length > 0 &&
              topicListArray.map(item => (
                <Option value={`${item}`} key={item}>
                  {item}
                </Option>
              ))}
            </Select>
          )}
        </FormItem>
      </Form>
    )
  }
}

SinkForm.propTypes = {
  locale: PropTypes.any,
  form: PropTypes.object,
  projectId: PropTypes.string,
  sinkList: PropTypes.object,
  topicList: PropTypes.object,
  sink: PropTypes.object,
  setBasicInfo: PropTypes.func,
  onGetTableSinks: PropTypes.func,
  onGetTableTopics: PropTypes.func,
  onSetSink: PropTypes.func
}
