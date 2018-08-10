import React, { PropTypes } from 'react'
import { connect } from 'react-redux'
import { createSelector } from 'reselect'
import { IntlProvider } from 'react-intl'
import { LocaleProvider } from 'antd'
import { makeSelectLocale } from './selectors'

export class LanguageProvider extends React.PureComponent {
  render () {
    return (
      <LocaleProvider locale={this.props.messages[this.props.locale].antdLocale}>
        <IntlProvider locale={this.props.locale} key={this.props.locale} messages={this.props.messages[this.props.locale]['message']}>
          {React.Children.only(this.props.children)}
        </IntlProvider>
      </LocaleProvider>
    )
  }
}

LanguageProvider.propTypes = {
  locale: PropTypes.string,
  messages: PropTypes.object,
  children: PropTypes.element.isRequired
}

const mapStateToProps = createSelector(
  makeSelectLocale(),
  (locale) => ({ locale })
)

export default connect(mapStateToProps)(LanguageProvider)
