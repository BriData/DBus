import React, {PropTypes, Component} from 'react'
import {connect} from 'react-redux'
import {createStructuredSelector} from 'reselect'
import Helmet from 'react-helmet'
// 导入自定义组件
import {
  Bread,
  KafkaReaderForm
} from '@/app/components'
import {makeSelectLocale} from '../LanguageProvider/selectors'
import {KafkaReaderModel} from './selectors'
import {
  getTopicList,
  readKafkaData,
  getOffsetRange
} from './redux'
// 链接reducer和action
@connect(
  createStructuredSelector({
    locale: makeSelectLocale(),
    KafkaReaderData: KafkaReaderModel()
  }),
  dispatch => ({
    getTopicList: param => dispatch(getTopicList.request(param)),
    readKafkaData: param => dispatch(readKafkaData.request(param)),
    getOffsetRange: param => dispatch(getOffsetRange.request(param)),
  })
)
export default class KafkaReaderWrapper extends Component {
  constructor(props) {
    super(props)
  }

  componentWillMount() {
    const {getTopicList} = this.props
    getTopicList()
  }

  handleRead = values => {
    const {readKafkaData} = this.props
    readKafkaData(values)
  }

  // handleGetOffsetRange = topic => {
  //   const {getOffsetRange} = this.props
  //   getOffsetRange({topic})
  // }

  render() {
    const topicList = (this.props.KafkaReaderData.topicList.result.payload || []).sort()
    const kafkaData = this.props.KafkaReaderData.kafkaData
    // const offsetRange = this.props.KafkaReaderData.offsetRange.result.payload || {}
    console.info(this.props)
    return (
      <div>
        <KafkaReaderForm
          topicList={topicList}
          kafkaData={kafkaData}
          // offsetRange={offsetRange}
          onRead={this.handleRead}
          // onGetOffsetRange={this.handleGetOffsetRange}
        />
      </div>
    )
  }
}
KafkaReaderWrapper.propTypes = {
  locale: PropTypes.any,
}
