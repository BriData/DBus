/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2018 Bridata
 * ==
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * >>
 */

package com.creditease.dbus.constant;

/**
 * User: 王少楠
 * Date: 2018-04-20
 * Time: 下午3:23
 */
public class MessageCode {
    //OK resultEntityBuilder默认值
    public static final int OK = 0;
    //EXCEPTION
    public static final int EXCEPTION = 100000;

    //User module code 10xxxx
    public static final int EMAIL_EMPTY = 100001;
    public static final int EMAIL_USED = 100002;
    public static final int USER_NOT_FOUND_OR_ID_EMPTY = 100003;
    public static final int USER_NOT_EXISTS = 100004;
    public static final int OLD_PASSWORD_WRONG = 100005;

    //Sink module code 11xxxx
    public static final int SINK_NAME_EMPTY = 110001;
    public static final int SINK_NAME_USED = 110002;
    public static final int SINK_NEW_EXCEPTION = 110003;
    public static final int SINK_IS_USING = 110004;
    public static final int SINK_URL_EMPTY = 110005;
    public static final int SINK_TYPE_EMPTY = 110006;
    public static final int SINK_DESC_EMPTY = 110007;

    //FullPull module code 12xxxx
    public static final int USER_ROLE_EMPTY = 120001;
    public static final int USER_ID_EMPTY = 120002;
    public static final int FULL_PULL_CODITION_ERROR = 120003;

    //ProjectTable module code 13xxx
    public static final int TABLE_ID_EMPTY = 130001;
    public static final int PROJECT_ID_EMPTY = 130002;
    public static final int TABLE_NOT_EXISTS = 130003;
    public static final int TABLE_ALREADY_EXISTS = 130004;
    public static final int TABLE_IS_RUNNING = 130005;

    //Resource module code 14xxx
    public static final int TABLE_ID_EMPTY_OR_PROJECT_ID_EMPTY = 140001;
    public static final int TABLE_DATA_FORMAT_ERROR = 140002;

    //Datasource module code 15xxx
    public static final int DATASOURCE_SOURCE_QUERY_FAILED = 150001;
    public static final int DATASOURCE_VALIDATE_FAILED = 150002;
    public static final int DATASOURCE_CHANGE_STATUS_FAILED = 150003;
    public static final int DATASOURCE_ALREADY_EXISTS = 150004;

    //Dataschema module code 16xxx
    public static final int DATASCHEMA_CHANGE_STATUS_FAILED = 160001;
    public static final int DATASCHEMA_FETCH_FROM_SOURCE_FAILED = 160002;
    public static final int DATASCHEMA_PARAM_FOARMAT_ERROR = 160003;
    public static final int DATASCHEMA_DS_TYPE_ERROR = 160004;

    //Table module code 17xxx
    public static final int TABLE_NOT_FOUND_BY_ID = 170001;
    public static final int CAN_NOT_FETCH_TABLE_COLUMNS_INFORMATION = 170002;
    public static final int SEND_CONFIRMATION_MESSAGE_FAILED = 170003;
    public static final int USE_THE_DIFF_CONTRAST  = 170004;
    public static final int FETCH_TABLE_ERROR  = 170005;
    public static final int SOURCE_TABLE_INSERT_ERROR  = 170006;
    public static final int TABLE_SUPPLEMENTAL_LOG_NOT_OPEN  = 170007;

    //Jar module code 18xxx
    public static final int PLUGIN_IS_USING  = 180001;
    public static final int ENCODE_PLUGIN_JAR_IS_WRONG  = 180002;
}
