import { createStore, applyMiddleware, compose } from 'redux'
import { fromJS } from 'immutable'
import { routerMiddleware } from 'react-router-redux'
import createSagaMiddleware from 'redux-saga'
import createReducer from './reducers'
import sagas from './sagas'

const sagaMiddleware = createSagaMiddleware()

export default function configureStore (initialState = {}, history) {
  const middlewares = [
    sagaMiddleware,
    routerMiddleware(history)
  ]

  const enhancers = [
    applyMiddleware(...middlewares)
  ]

  const composeEnhancers =
    process.env.NODE_ENV !== 'production' &&
    typeof window === 'object' && window.__REDUX_DEVTOOLS_EXTENSION_COMPOSE__
      ? window.__REDUX_DEVTOOLS_EXTENSION_COMPOSE__
      : compose

  const store = createStore(
    createReducer(),
    fromJS(initialState),
    composeEnhancers(...enhancers)
  )

  store.runSaga = sagaMiddleware.run
  store.asyncReducers = {}
  sagas.map(store.runSaga)

  /* istanbul ignore next */
  if (module.hot) {
    module.hot.accept('./reducers', () => {
      import('./reducers').then((reducerModule) => {
        const createReducers = reducerModule.default
        const nextReducers = createReducers(store.asyncReducers)

        store.replaceReducer(nextReducers)
      })
    })
  }

  return store
}
