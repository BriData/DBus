
/**
 * 国际化字符串
 * @param locale 存储的 locale
 * @param message {zh,en,...} 字符串信息
 */
export default (locale) => (message) => {
  if (locale) { return message[locale] } else { return message['zh'] }
}
