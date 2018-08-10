/**
 * i18n.js
 *
 * This will setup the i18n language files and locale data for your app.
 *
 */
import { addLocaleData } from 'react-intl'
import enLocaleData from 'react-intl/locale-data/en'
import zhLocaleData from 'react-intl/locale-data/zh'
import IntlMessageFormat from 'intl-messageformat'

import { DEFAULT_LOCALE } from '../app/containers/App/constants'

// 导入自定义国际化 JSON
import en from './translations/en.json'
import zh from './translations/zh.json'
// 导入antd国际化
import enUS from 'antd/lib/locale-provider/en_US'

export const appLocales = [
  'en',
  'zh'
]

export const formatTranslationMessages = (locale, messages) => {
  // locale
  switch (locale) {
    case 'en':
      addLocaleData(zhLocaleData)
      break
    case 'zh':
      addLocaleData(zhLocaleData)
      break
    default:
      addLocaleData(enLocaleData)
      break
  }

  const defaultFormattedMessages = locale !== DEFAULT_LOCALE
    ? formatTranslationMessages(DEFAULT_LOCALE, en)
    : {}
  return Object.keys(messages).reduce((formattedMessages, key) => {
    const formattedMessage = !messages[key] && locale !== DEFAULT_LOCALE
      ? defaultFormattedMessages[key]
      : messages[key]
    return Object.assign(formattedMessages, { [key]: formattedMessage })
  }, {})
}

export const translationMessages = {
  [appLocales[0]]: {
    antdLocale: enUS,
    message: formatTranslationMessages([appLocales[0]], en)
  },
  [appLocales[1]]: {
    antdLocale: null,
    message: formatTranslationMessages([appLocales[1]], zh)
  }
}

/**
 * 格式化字符串
 * @param  locale  [object String] local
 * @param  messages [object Object] 默认 null 定义国际化信息
 * @returns 返回一个字符串，如果messages存在则从自定义好的messages里面取值
 */
export const intlMessage = (locale, messages) => ({id, defaultMessage, valus}) => {
  let translationMessages = messages ? {en: {...en, ...messages.en}, zh: {...zh, ...messages.zh}} : {en, zh}
  let msg = locale ? translationMessages[locale][id] : translationMessages['zh'][id]
  if (msg == null) {
    if (defaultMessage != null) {
      return defaultMessage
    }
    return id
  }
  if (valus) {
    msg = new IntlMessageFormat(msg, locale)
    return msg.format(valus)
  }
  return msg
}
