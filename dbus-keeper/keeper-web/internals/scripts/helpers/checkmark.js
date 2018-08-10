const chalk = require('chalk')

function addCheckMark (callback) {
  process.stdout.write(chalk.green(' ✓'))
  if (callback) callback()
}

module.exports = addCheckMark
