import React, {PropTypes, Component} from 'react'
import {Modal, Form, Select, Input, Button, message, Table} from 'antd'
import {FormattedMessage} from 'react-intl'
import OperatingButton from '@/app/components/common/OperatingButton'

// 导入样式
import styles from './res/styles/index.less'
import Request from "@/app/utils/request";
import {
  CHECK_GLOBAL_FULLPULL_TOPO_API,
  KILL_GLOBAL_FULLPULL_TOPO_API
} from '@/app/containers/toolSet/api'
const FormItem = Form.Item
const Option = Select.Option
const TextArea = Input.TextArea

@Form.create()
export default class GlobalFullpullStartTopo extends Component {
  constructor(props) {
    super(props)
    this.state = {
      // 用于筛选下拉列表选项
      version: null,
      type: 'splitter_puller',
      loading: false,

      status: false
    }
  }

  componentWillMount = () => {
    this.handleRefreshTopoStatus()
  }

  handleRefreshTopoStatus = () => {
    Request(CHECK_GLOBAL_FULLPULL_TOPO_API, {
      method: 'get'
    })
      .then(res => {
        if (res && res.status === 0) {
          this.setState({
            status: res.payload
          })
        } else {
          message.warn(res.message)
        }
      })
      .catch(error => {
        error.response && error.response.data && error.response.data.message
          ? message.error(error.response.data.message)
          : message.error(error.message)
      })
  }

  handleVersionChange = value => {
    this.setState({version: value})
    this.props.form.setFieldsValue({minorVersion: null})
  }

  handleTypeChange = value => {
    // this.setState({type: value})
    // this.props.form.setFieldsValue({minorVersion: null})
  }

  handleKill = () => {
    Request(KILL_GLOBAL_FULLPULL_TOPO_API, {
      method: 'get'
    })
      .then(res => {
        if (res && res.status === 0) {
          message.success(res.message)
        } else {
          message.warn(res.message)
        }
      })
      .catch(error => {
        error.response && error.response.data && error.response.data.message
          ? message.error(error.response.data.message)
          : message.error(error.message)
      })
  }

  handleStart = () => {
    this.props.form.validateFieldsAndScroll((err, values) => {
      if (!err) {
        this.props.form.setFieldsValue({log: null})
        const {topoJarStartApi, jarInfos} = this.props
        const jar = jarInfos.filter(jar => jar.version === values.version && jar.type === values.type && jar.minorVersion === values.minorVersion)[0]

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
    const {loading, status} = this.state
    const {jarInfos} = this.props
    const {getFieldDecorator} = this.props.form
    const {version, type} = this.state
    const versionList = Array.from(new Set(jarInfos.map(jar => jar.version)))
    const typeList = Array.from(new Set(jarInfos.filter(jar => jar.version === version).map(jar => jar.type)))
    const minorVersionList = Array.from(new Set(jarInfos.filter(jar => jar.version === version && jar.type === type).map(jar => jar.minorVersion)))
    const formItemLayout = {
      labelCol: {span: 4},
      wrapperCol: {span: 10}
    }
    const tailItemLayout = {
      labelCol: {span: 4},
      wrapperCol: {offset: 4,span: 10}
    }
    return (
      <div className={styles.table}>
        <Form autoComplete="off" className={styles.register}
        >
          <FormItem label={<FormattedMessage
            id="app.components.resourceManage.dataSourceName"
            defaultMessage="数据源名称"
          />} {...formItemLayout}>
            {getFieldDecorator('dsName', {
              initialValue: 'global',
            })(<Input disabled={true} size="large" type="text"/>)}
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
              id="app.common.type"
              defaultMessage="类型"
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
                disabled
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
              id="app.components.toolset.globalFullPull.minorVersion"
              defaultMessage="上传时间"
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
              >
                {minorVersionList.map(minorVersion => (
                  <Option value={minorVersion} key={minorVersion}>{minorVersion}</Option>
                ))}
              </Select>
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
            <Button disabled={status} type="primary" loading={loading} onClick={this.handleStart}>
              <FormattedMessage
                id="app.components.resourceManage.dataTable.start"
                defaultMessage="启动"
              />
            </Button>
            <Button style={{marginLeft: 10}} disabled={!status} type="primary" onClick={this.handleKill}>
              <FormattedMessage
                id="app.components.resourceManage.dataTable.stop"
                defaultMessage="停止"
              />
            </Button>
            <Button style={{marginLeft: 10}} onClick={this.handleRefreshTopoStatus}>
              {status ?
                <FormattedMessage
                  id="app.components.toolset.globalFullPull.refreshAlreadyStart"
                  defaultMessage="刷新（当前状态：已启动）"
                />
                :
                <FormattedMessage
                  id="app.components.toolset.globalFullPull.refreshNotStart"
                  defaultMessage="刷新（当前状态：未启动）"
                />
              }
            </Button>
          </FormItem>

        </Form>
      </div>
    )
  }
}

GlobalFullpullStartTopo.propTypes = {}
