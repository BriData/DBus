import React, {PropTypes, Component} from 'react'
import {Modal, Form, Select, Input, Button, message, Table, Popconfirm} from 'antd'
import {FormattedMessage} from 'react-intl'

// 导入样式
import styles from './res/styles/index.less'
import Request from "@/app/utils/request";
import {TOPO_JAR_START_API} from "@/app/containers/ResourceManage/api";

const FormItem = Form.Item
const Option = Select.Option

@Form.create()
export default class BathchRestatrTopoRestart extends Component {
  constructor(props) {
    super(props)
    this.state = {
      // 用于筛选下拉列表选项
      version: null,
      type: null,
      loading: false,
    }
  }

  handleVersionChange = value => {
    this.setState({version: value})
    this.props.form.setFieldsValue({minorVersion: null})
  }

  handleTypeChange = value => {
    this.setState({type: value})
    this.props.form.setFieldsValue({version: null, minorVersion: null})
  }

  restart = () => {
    this.props.form.validateFields((err, values) => {
      if (!err) {
        this.handleReStart()
      }
    })
  }

  handleReStart = () => {
    this.props.form.validateFieldsAndScroll((err, values) => {
      if (!err) {
        this.props.form.setFieldsValue({log: null})
        const {batchRestartTopoApi, selectedRows, params} = this.props
        const {jarInfos} = this.props
        const jar = jarInfos.filter(jar => jar.version === values.version && jar.type === values.type && jar.minorVersion === values.minorVersion)[0]
        const dsNameList = Array.from(new Set(selectedRows.map(row => row.name)))
        const dsType = params.dsType

        this.setState({loading: true})
        Request(batchRestartTopoApi, {
          data: {
            dsType: dsType,
            dsNameList: dsNameList,
            jarPath: jar.path,
            topoType: values.type.replace('_', '-')
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

  componentWillMount() {
    const {onSearchJarInfos} = this.props
    onSearchJarInfos()
  }

  render() {
    const {loading} = this.state
    const {key, visible, onClose, jarInfos, selectedRows} = this.props
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
              id="app.components.toolset.BatchRestartTopo.restart"
              defaultMessage="批量启动拓扑"
            />
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
            <FormItem
              label={<FormattedMessage
                id="app.components.toolset.BatchRestartTopo.topoType"
                defaultMessage="拓扑类型"
              />} {...formItemLayout}
            >
              {getFieldDecorator('type', {
                initialValue: type,
                rules: [
                  {
                    required: true,
                    message: 'topoType不能为空'
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
                >
                  {minorVersionList.map(minorVersion => (
                    <Option value={minorVersion} key={minorVersion}>{minorVersion}</Option>
                  ))}
                </Select>
              )}
            </FormItem>
            <FormItem {...tailItemLayout}>
              <Popconfirm title={'该操作为异步执行,请确认你已经打开mgr日志,查看重启结果!!'} onConfirm={this.restart} okText="Yes"
                          cancelText="No">
                <Button type="primary" loading={loading}>
                  <FormattedMessage
                    id="app.components.resourceManage.dataTable.start"
                    defaultMessage="启动"
                  />
                </Button>
              </Popconfirm>
            </FormItem>
          </Form>
        </Modal>
      </div>
    )
  }
}

BathchRestatrTopoRestart.propTypes = {}
