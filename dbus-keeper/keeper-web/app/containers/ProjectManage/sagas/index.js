// project home
import ProjectHomeSaga from '@/app/components/ProjectManage/ProjectHome/saga'
// project resource
import ProjectResourceSaga from '@/app/components/ProjectManage/ProjectResource/saga'
// project summary
import ProjectSummarySaga from '@/app/components/ProjectManage/ProjectSummary/saga'
// project table
import ProjectTableSaga from '@/app/components/ProjectManage/ProjectTable/saga'
// project table
import ProjectTopologySaga from '@/app/components/ProjectManage/ProjectTopology/saga'
// project fullpull history
import ProjectFullpullSaga from '@/app/components/ProjectManage/ProjectFullpull/saga'

export default [
  ...ProjectHomeSaga,
  ...ProjectResourceSaga,
  ...ProjectSummarySaga,
  ...ProjectTableSaga,
  ...ProjectTopologySaga,
  ...ProjectFullpullSaga
]
