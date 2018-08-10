const express = require('express')
const path = require('path')
const compression = require('compression')
const pkg = require(path.resolve(process.cwd(), 'package.json'))
const proxy = require('http-proxy-middleware')

const addDevMiddlewares = (app, webpackConfig) => {
  const webpack = require('webpack')
  const webpackDevMiddleware = require('webpack-dev-middleware')
  const webpackHotMiddleware = require('webpack-hot-middleware')
  const compiler = webpack(webpackConfig)
  const middleware = webpackDevMiddleware(compiler, {
    noInfo: true,
    publicPath: webpackConfig.output.publicPath,
    silent: true,
    stats: 'errors-only'
  })
  // 静态资源
  app.use(express.static(path.join(__dirname, '..', '..', 'static')))

  app.use(['/keeper'], proxy({target: 'http://localhost:5090/v1', changeOrigin: true}))
  app.use(proxy('/keeper-webSocket',{
    target: 'http://localhost:8902',
    changeOrigin: true,
    ws: true,
    pathRewrite: {
      '^/keeper-webSocket' : '',
    },
    logLevel: 'debug'
  }))
  app.use(middleware)
  app.use(webpackHotMiddleware(compiler))

  const fs = middleware.fileSystem

  if (pkg.dllPlugin) {
    app.get(/\.dll\.js$/, (req, res) => {
      const filename = req.path.replace(/^\//, '')
      res.sendFile(path.join(process.cwd(), pkg.dllPlugin.path, filename))
    })
  }

  app.get('*', (req, res) => {
    fs.readFile(path.join(compiler.outputPath, 'index.html'), (err, file) => {
      if (err) {
        res.sendStatus(404)
      } else {
        res.send(file.toString())
      }
    })
  })
}

const addProdMiddlewares = (app, options) => {
  const publicPath = options.publicPath || '/'
  const outputPath = options.outputPath || path.resolve(process.cwd(), 'build')

  app.use(compression())
  app.use(publicPath, express.static(outputPath))

  app.get('*', (req, res) => res.sendFile(path.resolve(outputPath, 'index.html')))
}

module.exports = (app, options) => {
  const isProd = process.env.NODE_ENV === 'production'

  if (isProd) {
    addProdMiddlewares(app, options)
  } else {
    const webpackConfig = require('../../internals/webpack/webpack.dev.babel')
    addDevMiddlewares(app, webpackConfig)
  }

  return app
}
