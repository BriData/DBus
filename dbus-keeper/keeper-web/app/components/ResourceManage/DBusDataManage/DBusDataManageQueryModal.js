import React, {PropTypes, Component} from 'react'
import {Button, Row, Col, Modal, Form, Select, Input, Spin, Table, Icon, message} from 'antd'
import {FormattedMessage} from 'react-intl'
import OperatingButton from '@/app/components/common/OperatingButton'

// 导入样式
import styles from './res/styles/index.less'
import Request from "@/app/utils/request";

const FormItem = Form.Item
const Option = Select.Option
const TextArea = Input.TextArea

@Form.create()
export default class DBusDataManageQueryModal extends Component {
  constructor(props) {
    super(props)
  }

  componentDidMount = () => {
    this.handleExecuteSql('master')
  }

  /**
   * @param render 传入一个render
   * @returns render 返回一个新的render
   * @description 统一处理render函数
   */
  renderComponent = render => (text, record, index) =>
    render(text, record, index);

  /**
   * @description 默认的render
   */
  renderNomal = width => (text, record, index) => (
    <div
      title={text}
      style={{width}}
      className={styles.ellipsis}
    >
      {text}
    </div>
  );

  handleExecuteSql = type => {
    this.props.form.validateFields((err, values) => {
      if (!err) {
        const {onExecuteSql} = this.props
        onExecuteSql({
          ...values,
          type
        })
      }
    })
  }

  generateColumns = result => {
    let keys = new Set()
    result.forEach(line => {
      Object.keys(line).forEach(key => {
        keys.add(key)
      })
    })
    keys = Array.from(keys)
    keys = keys.map(key => ({
      title: key,
      key: key,
      dataIndex: key,
      render: this.renderComponent(this.renderNomal(150))
    }))
    return keys
  }

  render() {
    const { getFieldDecorator } = this.props.form
    const {visible, key,sql, loading, onClose, executeResult} = this.props
    const columns = this.generateColumns(executeResult)
    const formItemLayout = {
      labelCol: {
        span: 2
      },
      wrapperCol: {
        span: 22
      },
    }
    return (
      <Modal
        className="top-modal"
        visible={visible}
        maskClosable={false}
        key={key}
        onCancel={onClose}
        footer={[<Button type="primary" onClick={onClose}>
          <FormattedMessage
            id="app.common.back"
            defaultMessage="返回"
          />
        </Button>]}
        width={1000}
        title={<FormattedMessage
          id="app.components.resourceManage.dbusData.sourceDataInsight"
          defaultMessage="查询源端数据"
        />}
      >
        <Form autoComplete="off" className={styles.searchForm}>
          <Row>
            <Col span={24}>
              <FormItem style={{marginTop: -5}} label={'SQL'} {...formItemLayout}>
                {getFieldDecorator('sql', {
                  initialValue: sql,
                  rules: [
                    {
                      required: true,
                      message: 'SQL不能为空'
                    }
                  ]
                })(<TextArea
                  autosize={{minRows:2}}
                />)}
              </FormItem>
              <FormItem style={{marginTop: -20, marginBottom: 5}} wrapperCol={{offset: 17, span: 7}}>
                <Button style={{marginLeft: 10}} onClick={() => this.handleExecuteSql('master')}><FormattedMessage
                  id="app.components.resourceManage.dbusData.queryMaster"
                  defaultMessage="查询主库"
                /></Button>
                <Button style={{marginLeft: 10}} onClick={() => this.handleExecuteSql('slave')}><FormattedMessage
                  id="app.components.resourceManage.dbusData.querySlave"
                  defaultMessage="查询备库"
                /></Button>
              </FormItem>
            </Col>
          </Row>
        </Form>
        <Spin spinning={loading} tip="正在加载数据中...">
          {!loading ? (
            <Table
              rowKey={record => JSON.stringify(record)}
              size="small"
              columns={columns}
              dataSource={executeResult}
              scroll={{x: true}}
            />
          ) : (
            <div style={{width: '100%', height: 100}}/>
          )}
        </Spin>
      </Modal>
    )
  }
}

DBusDataManageQueryModal.propTypes = {}
