export function createActionTypes (base, actions = []) {
  return actions.reduce((acc, type) => {
    acc[type] = `${base}_${type}`
    return acc
  }, {})
}

export function createAction (type, data = {}) {
  return { type, result: data }
}
