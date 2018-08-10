const express = require('express')
const logger = require('./logger')

const argv = require('minimist')(process.argv.slice(2))
const setup = require('./middlewares/frontendMiddleware')
const resolve = require('path').resolve
const app = express()

setup(app, {
  outputPath: resolve(process.cwd(), 'build'),
  publicPath: '/'
})

const customHost = argv.host || process.env.HOST
const host = customHost || null
const prettyHost = customHost || 'localhost'

const port = argv.port || process.env.PORT || 3088

app.listen(port, host, (err) => {
  if (err) {
    return logger.error(err.message)
  }

  logger.appStarted(port, prettyHost)
})
