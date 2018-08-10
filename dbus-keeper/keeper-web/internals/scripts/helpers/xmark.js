const chalk = require('chalk')

function addXMark (callback) {
  process.stdout.write(chalk.red(' ✘'))
  if (callback) callback()
}

module.exports = addXMark
