'use strict'

const readline = require('readline')

function animateProgress (message, amountOfDots) {
  if (typeof amountOfDots !== 'number') {
    amountOfDots = 3
  }

  let i = 0
  return setInterval(function () {
    readline.cursorTo(process.stdout, 0)
    i = (i + 1) % (amountOfDots + 1)
    const dots = new Array(i + 1).join('.')
    process.stdout.write(message + dots)
  }, 500)
}

module.exports = animateProgress
