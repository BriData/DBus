# -*- coding: UTF-8 -*-
# Copyright 2013-2014 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import threading

from mongo_connector.MessageStatusQueue import MessageStatusQueue

# 默认为true, kafka_doc出或ctrl reader出错退出时候，设为false
thread_alive = True

# 已经通过kafka发送的数据的状态
message_status_queue = MessageStatusQueue()

# 用于表示缓存已经变了，需要重新loadschemaTables_set
# 在 kafka_ctrl_topic_reader 收到reload event时，设为 true
# 建议在OplogThread 中判断为true时，进行加载reload schemaTables, reload结束后，设置为false
need_reload = True

# 20180320： remove global_variable.schemaTables_set
# # 经过分析：thread_lock和schemaTables_set可以不用存在, 直接使用 kafka_doc_mananger的 _cache_schemaTables_set
# # schema table的全局锁
# thread_lock = threading.Lock()
# # 缓存当前支持的schema table，用于过滤不需要的表使用
# schemaTables_set = set()
