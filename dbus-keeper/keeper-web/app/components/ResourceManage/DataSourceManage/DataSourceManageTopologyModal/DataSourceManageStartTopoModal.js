import React, {Component} from 'react'
import {Button, Form, Input, message, Modal, Select} from 'antd'
import {FormattedMessage} from 'react-intl'
// 导入样式
import styles from '../res/styles/index.less'
import Request from "@/app/utils/request";

const FormItem = Form.Item
const Option = Select.Option
const TextArea = Input.TextArea

@Form.create()
export default class DataSourceManageStartTopoModal extends Component {
  constructor(props) {
    super(props)
    this.state = {
      // 用于筛选下拉列表选项
      version: null,
      type: null,
      loading: false,
      minorVersion: null,
      jar: null
    }
  }

  componentWillMount = () => {
    const {record, dataSource} = this.props
    const dsName = dataSource.name || ''
    const topoName = record.topologyName || ''
    const initialType = topoName.substr(dsName.length + 1).replace(/-/g, '_')
    this.setState({
      type: initialType
    })
  }

  handleVersionChange = value => {
    this.setState({version: value})
    this.props.form.setFieldsValue({minorVersion: null})
  }

  handleMinorVersionChange = value => {
    this.setState({minorVersion: value})
    const {jarInfos} = this.props
    const {version, type} = this.state
    if (type && version && value) {
      const jar = jarInfos.filter(jar => jar.version === version && jar.type === type && jar.minorVersion === value)[0]
      this.setState({jar: jar})
      console.log(jar)
      this.props.form.setFieldsValue({
        description: jar && jar.description,
      })
    }
  }

  handleTypeChange = value => {
    this.setState({type: value})
    this.props.form.setFieldsValue({version: null, minorVersion: null, description: null})
  }

  handleStart = () => {
    this.props.form.validateFieldsAndScroll((err, values) => {
      if (!err) {
        this.props.form.setFieldsValue({log: null})
        const {topoJarStartApi} = this.props
        // const {jarInfos} = this.props
        // const jar = jarInfos.filter(jar => jar.version === values.version && jar.type === values.type && jar.minorVersion === values.minorVersion)[0]
        const {jar} = this.state

        this.setState({loading: true})
        Request(topoJarStartApi, {
          data: {
            dsName: values.dsName,
            jarPath: jar.path,
            jarName: jar.fileName,
            topologyType: values.type.replace('_', '-')
          },
          method: 'post'
        })
          .then(res => {
            this.setState({loading: false})
            if (res && res.status === 0) {
              message.success(res.message)
              this.props.form.setFieldsValue({log: res.payload})
            } else {
              message.warn(res.message)
            }
          })
          .catch(error => {
            this.setState({loading: false})
            error.response && error.response.data && error.response.data.message
              ? message.error(error.response.data.message)
              : message.error(error.message)
          })
      }
    })
  }

  render() {
    const {loading, description} = this.state
    const {key, visible, record, dataSource, onClose} = this.props
    const {jarInfos} = this.props
    const {getFieldDecorator} = this.props.form
    const {version, type} = this.state
    const typeList = Array.from(new Set(jarInfos.map(jar => jar.type)))
    const versionList = Array.from(new Set(jarInfos.filter(jar => jar.type === type).map(jar => jar.version)))
    const minorVersionList = Array.from(new Set(jarInfos.filter(jar => jar.version === version && jar.type === type).map(jar => jar.minorVersion)))
    const formItemLayout = {
      labelCol: {
        xs: {span: 5},
        sm: {span: 6}
      },
      wrapperCol: {
        xs: {span: 19},
        sm: {span: 12}
      }
    }
    const tailItemLayout = {
      wrapperCol: {
        xs: {offset: 5, span: 19},
        sm: {offset: 6, span: 12}
      }
    }

    return (
      <div className={styles.table}>
        <Modal
          key={key}
          className="top-modal"
          visible={visible}
          maskClosable={false}
          width={1000}
          title={<div>
            <FormattedMessage
              id="app.components.toolset.globalFullPull.startTopology"
              defaultMessage="启动拓扑"
            /> --- {record.topologyName}
          </div>}
          onCancel={onClose}
          footer={[<Button type="primary" onClick={onClose}>
            <FormattedMessage
              id="app.common.back"
              defaultMessage="返回"
            />
          </Button>]}
        >
          <Form autoComplete="off"
                className="data-source-start-topo-form"
          >
            <FormItem label={<FormattedMessage
              id="app.components.resourceManage.dataSourceName"
              defaultMessage="数据源名称"
            />} {...formItemLayout}>
              {getFieldDecorator('dsName', {
                initialValue: dataSource.name,
              })(<Input disabled={true} size="large" type="text"/>)}
            </FormItem>

            <FormItem
              label={<FormattedMessage
                id="app.components.resourceManage.dataSourceType"
                defaultMessage="数据源类型"
              />} {...formItemLayout}
            >
              {getFieldDecorator('type', {
                initialValue: type,
                rules: [
                  {
                    required: true,
                    message: 'type不能为空'
                  }
                ]
              })(
                <Select
                  showSearch
                  optionFilterProp='children'
                  className={styles.select}
                  placeholder="Select type"
                  onChange={this.handleTypeChange}
                >
                  {typeList.map(type => (
                    <Option value={type} key={type}>{type}</Option>
                  ))}
                </Select>
              )}
            </FormItem>

            <FormItem
              label={<FormattedMessage
                id="app.common.version"
                defaultMessage="版本"
              />} {...formItemLayout}
            >
              {getFieldDecorator('version', {
                initialValue: null,
                rules: [
                  {
                    required: true,
                    message: 'version不能为空'
                  }
                ]
              })(
                <Select
                  showSearch
                  optionFilterProp='children'
                  className={styles.select}
                  placeholder="Select version"
                  onChange={this.handleVersionChange}
                >
                  {versionList.map(version => (
                    <Option value={version} key={version}>{version}</Option>
                  ))}
                </Select>
              )}
            </FormItem>

            <FormItem
              label={<FormattedMessage
                id="app.components.resourceManage.minorVersion"
                defaultMessage="小版本"
              />} {...formItemLayout}
            >
              {getFieldDecorator('minorVersion', {
                initialValue: null,
                rules: [
                  {
                    required: true,
                    message: 'minor version不能为空'
                  }
                ]
              })(
                <Select
                  showSearch
                  optionFilterProp='children'
                  className={styles.select}
                  placeholder="Select minor version"
                  onChange={this.handleMinorVersionChange}
                >
                  {minorVersionList.map(minorVersion => (
                    <Option value={minorVersion} key={minorVersion}>{minorVersion}</Option>
                  ))}
                </Select>
              )}
            </FormItem>

            <FormItem label={<FormattedMessage
              id="app.common.description"
              defaultMessage="描述"
            />} {...formItemLayout}
            >
              {getFieldDecorator('description', {
                initialValue: null,
              })(
                <Input size="large" disabled={true} type="text"/>
              )}
            </FormItem>

            <FormItem label={<FormattedMessage
              id="app.components.toolset.globalFullPull.startDescription"
              defaultMessage="启动说明"
            />} {...formItemLayout}>
              {getFieldDecorator('desc', {
                initialValue: null,
              })(<Input size="large" type="text"/>)}
            </FormItem>
            <FormItem label={<FormattedMessage
              id="app.common.log"
              defaultMessage="日志"
            />} {...formItemLayout}>
              {getFieldDecorator('log', {
                initialValue: null,
              })(<TextArea wrap='off' readOnly autosize={{minRows: 10, maxRows: 20}}/>)}

            </FormItem>
            <FormItem {...tailItemLayout}>
              <Button type="primary" loading={loading} onClick={this.handleStart}>
                <FormattedMessage
                  id="app.components.resourceManage.dataTable.start"
                  defaultMessage="启动"
                />
              </Button>
            </FormItem>

          </Form>
        </Modal>
      </div>
    )
  }
}

DataSourceManageStartTopoModal.propTypes = {}
