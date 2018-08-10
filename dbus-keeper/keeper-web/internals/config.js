const resolve = require('path').resolve
const pullAll = require('lodash/pullAll')
const uniq = require('lodash/uniq')

const edpReactSpaStarter = {
  version: '0.1.0',

  dllPlugin: {
    defaults: {
      exclude: [
        'chalk',
        'compression',
        'cross-env',
        'express',
        'ip',
        'minimist',
        'sanitize.css'
      ],

      include: ['core-js', 'eventsource-polyfill', 'babel-polyfill', 'lodash'],

      path: resolve('../node_modules/edp-react-spa-starter-dlls')
    },

    entry (pkg) {
      const dependencyNames = Object.keys(pkg.dependencies)
      const exclude = pkg.dllPlugin.exclude || edpReactSpaStarter.dllPlugin.defaults.exclude
      const include = pkg.dllPlugin.include || edpReactSpaStarter.dllPlugin.defaults.include
      const includeDependencies = uniq(dependencyNames.concat(include))

      return {
        edpReactSpaStarterDeps: pullAll(includeDependencies, exclude)
      }
    }
  }
}

module.exports = edpReactSpaStarter
