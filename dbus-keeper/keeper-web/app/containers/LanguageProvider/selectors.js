import { createSelector } from 'reselect'

const selectLanguage = (state) => state.get('language')

const makeSelectLocale = () => createSelector(
  selectLanguage,
  (languageState) => languageState.get('locale')
)

export {
  selectLanguage,
  makeSelectLocale
}
