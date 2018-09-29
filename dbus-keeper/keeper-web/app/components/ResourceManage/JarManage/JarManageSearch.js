import React, { PropTypes, Component } from 'react'
import {Popconfirm, Form, message, Select, Input, Button, Row, Col } from 'antd'
import { FormattedMessage } from 'react-intl'
// 导入样式
import styles from './res/styles/index.less'
import {
  JAR_GET_TYPE_LIST_API,
  JAR_GET_VERSION_LIST_API
} from "@/app/containers/ResourceManage/api";
import Request from "@/app/utils/request";

const FormItem = Form.Item
const Option = Select.Option

export default class JarManageSearch extends Component {
  constructor (props) {
    super(props)
    this.state = {
      versionList: [],
      typeList: []
    }
  }

  componentWillMount () {
    this.getVersionList()
  }

  getVersionList = (category = 'normal') => {
    Request(`${JAR_GET_VERSION_LIST_API}/${category}`, {
      method: 'get' })
      .then(res => {
        if (res && res.status === 0) {
          this.setState({
            versionList: res.payload,
            typeList: []
          })
        } else {
          message.warn(res.message)
        }
      })
      .catch(error => {
        error.response.data && error.response.data.message
          ? message.error(error.response.data.message)
          : message.error(error.message)
      })
  }

  handleCategoryChange = value => {
    const {onSearch, filterParams} = this.props
    onSearch({ ...filterParams, category: value, version: null, type: null })
    this.getVersionList(value)
  }

  handleVersionChange = value => {
    const {filterParams} = this.props
    const {category} = filterParams
    const {onSearchParamChange} = this.props
    onSearchParamChange({
      version: value,
      type: null
    })
    Request(`${JAR_GET_TYPE_LIST_API}`, {
      params: {
        category: category,
        version: value
      },
      method: 'get' })
      .then(res => {
        if (res && res.status === 0) {
          this.setState({
            typeList: res.payload
          })
        } else {
          message.warn(res.message)
        }
      })
      .catch(error => {
        error.response.data && error.response.data.message
          ? message.error(error.response.data.message)
          : message.error(error.message)
      })
  }

  handleSearch = () => {
    const { onSearch } = this.props
    const { filterParams } = this.props
    onSearch({ ...filterParams })
  };

  render () {
    const {onBatchDelete, onUploadJar, onSearchParamChange, filterParams} = this.props
    const {category, version, type} = filterParams
    const {versionList, typeList} = this.state
    return (
      <div className="form-search">
        <Form autoComplete="off" layout="inline" className={styles.searchForm}>
          <Row>
            <Col span={5} className={styles.formLeft}>
              <FormItem>
                <Popconfirm title={'确认批量删除？'} onConfirm={onBatchDelete} okText="Yes" cancelText="No">
                  <Button
                    type="primary"
                    size="large"
                  >
                    <FormattedMessage
                      id="app.components.resourceManage.jarManager.batchDelete"
                      defaultMessage="批量删除"
                    />
                  </Button>
                </Popconfirm>
              </FormItem>
              <FormItem>
                <Button
                  type="primary"
                  size="large"
                  onClick={onUploadJar}
                >
                  <FormattedMessage
                    id="app.components.resourceManage.jarManager.uploadJar"
                    defaultMessage="上传Jar包"
                  />
                </Button>
              </FormItem>
            </Col>
            <Col span={7}>
              <FormItem label={<FormattedMessage
                id="app.common.type"
                defaultMessage="类型"
              />}>
                <Select
                  showSearch
                  optionFilterProp='children'
                  className={styles.select}
                  placeholder="Select category"
                  value={category}
                  onChange={value => this.handleCategoryChange(value)}
                >
                  <Option value='normal' key='normal'>
                    normal
                  </Option>
                  <Option value='router' key='router'>
                    router
                  </Option>
                </Select>
              </FormItem>
            </Col>
            <Col className={styles.formRight} span={12}>
              <FormItem>
                <Select
                  showSearch
                  optionFilterProp='children'
                  className={styles.select}
                  placeholder="Select version"
                  value={version}
                  onChange={value => this.handleVersionChange(value)}
                >
                  {versionList.map(version => (
                    <Option value={version} key={version}>{version}</Option>
                  ))}
                </Select>
              </FormItem>
              <FormItem>
                <Select
                  showSearch
                  optionFilterProp='children'
                  className={styles.select}
                  placeholder="Select type"
                  value={type}
                  onChange={value =>
                    onSearchParamChange({
                      type: value
                    })}
                >
                  {typeList.map(type => (
                    <Option value={type} key={type}>{type}</Option>
                  ))}
                </Select>
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

JarManageSearch.propTypes = {
}
