import 'antd/dist/antd.css'
import './assets/css/index.css'
import './assets/css/style.css'
import './assets/css/filepicker.css'
import './assets/css/dropzone.css'
import './assets/css/dropzone.min.css'
import './assets/css/font-awesome.css'
import './assets/css/login.css'
import 'bootstrap/dist/css/bootstrap.css'
import 'bootstrap/dist/css/bootstrap-theme.css'
import 'fixed-data-table/dist/fixed-data-table.css'
import 'jsoneditor/dist/jsoneditor.css'

import './assets/css/react-contextmenu.css';
import './assets/css/treeview.css'

import './components/common/jquery-extends'
import React from 'react'
import ReactDOM from 'react-dom'
import {Router, Route, IndexRoute, hashHistory} from 'react-router'
import App from './components/app'
import Schema from './components/data-schema/schema'
import DataSource from './components/datasource/datasource'
import DataTable from './components/data-table/data-table'
import AvroSchema from './components/avro-schema/avro-schema'
import UpdateDs from './components/datasource/datasource-update'
import UpdateSchema from './components/data-schema/schema-update'
import EditRule from './components/data-table/edit-rule'
import ConfigRule from './components/data-table/config-rule'
import UpdateTable from './components/data-table/data-table-update'
import TableVersionDifference from './components/data-table/data-table-version-difference'
import ModifySchemaTable from './components/modify/schema-table/modify-schema-table'
import PlainlogModifySchemaTable from './components/modify/plainlog-schema-table/plainlog-modify-schema-table'
import Start from './components/new-ds/step-1'
import Second from './components/new-ds/step-2'
import SecondLogProcessor from './components/new-ds/step-2-log-processor'
import Topologys from './components/topology/topology'
import ControlMessage from './components/control-message/ctl-message'


import JarMgr from './components/jar-mgr/jar-mgr'
import ZkManager from './components/zk-manager/zk-manager'
import DsZkConf from './components/new-ds/step-ds-zkconf'
import DbusData from './components/dbus-data/dbus-data'
import StartTopo from './components/new-ds/step-start-topos'
import Monitor from './components/monitor/Monitor'
import Login from './components/login/login'
import Signup from './components/signup/signup'
import FullPull from './components/fullPull-independently/full-pull'
import FullPullHistory from './components/fullpullhistory/fullpullhistory'
import Config from './components/config/config'

ReactDOM.render(
    <Router history={hashHistory}>
        <Route path='/login' component={Login}/>
        <Route path='/signup' component={Signup}/>
        <Route path='/' component={App}>
            <IndexRoute component={DataSource}/>
            <Route path='/datasource'>
                <IndexRoute component={DataSource}/>
                <Route path='/datasource/ds-update' component={UpdateDs}/>
                <Route path='/modify/schema-table' component={ModifySchemaTable}/>
            </Route>
            <Route path='/config' component={Config}/>
            <Route path='/data-schema'>
                <IndexRoute component={Schema}/>
                <Route path='/data-schema/schema-update' component={UpdateSchema}/>
                <Route path='/data-schema/modify/schema-table' component={ModifySchemaTable}/>
                <Route path='/data-schema/modify/plainlog-schema-table' component={PlainlogModifySchemaTable}/>
            </Route>
            <Route path='/data-table'>
                <IndexRoute component={DataTable}/>
                <Route path='/data-table/table-update' component={UpdateTable}/>
                <Route path='/data-table/table-version-difference' component={TableVersionDifference}/>
                <Route path='/data-table/edit-rule' component={EditRule}/>
                <Route path='/data-table/config-rule' component={ConfigRule}/>
            </Route>
            <Route path='/nds-step'>
                <IndexRoute component={Start}/>
                <Route path='/nds-step/nds-first' component={Start}/>
                <Route path='/nds-step/nds-second' component={Second}/>
                <Route path='/nds-step/nds-second-log-processor' component={SecondLogProcessor}/>
                <Route path='/nds-step/nds-zkConf' component={DsZkConf}/>
                <Route path='/nds-step/nds-start-topos' component={StartTopo}/>
            </Route>
            <Route path='/avro' component={AvroSchema}/>
            <Route path='/topology' component={Topologys}/>
            <Route path='/controlMessage' component={ControlMessage}/>
            <Route path='/jar-mgr' component={JarMgr}/>
            <Route path='/zk-manager' component={ZkManager}/>
            <Route path='/dbus-data' component={DbusData}/>
            <Route path='/monitor' component={Monitor}/>
            <Route path='/full-pull' component={FullPull}/>
            <Route path='/fullpull-history' component={FullPullHistory}/>
        </Route>
    </Router>,
    document.getElementById('root')
)
