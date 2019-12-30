# -*- coding: utf-8-*-

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
import time
import logging

import global_variable

from zk_helper import ZooKeeperHelper
from mysqldb_helper import MysqlDBHelper
from kafka_client_creator import KafkaClientCreator

LOG = logging.getLogger(__name__)


# 2 Usages:
#    1) poll ctrl event for ctrl topic
#    2) receive reload event, load schemaTable set.
class KafkaCtrlTopicReader(threading.Thread):
    def __init__(self, db, consumer_props):
        kafka = KafkaClientCreator(db.get_datasource_name())
        self._consumer = kafka.create_consumer([db.get_ctrl_topic()], consumer_props)

        # 20180320： remove global_variable.schemaTables_set
        # # initial load global schema table set
        # self._db = db
        # tables_set = self._db.load_schemaTables()
        # global_variable.thread_lock.acquire()
        # global_variable.schemaTables_set = tables_set
        # global_variable.thread_lock.release()
        # LOG.info("mongo extractor initial read config : " + str(global_variable.schemaTables_set))
        global_variable.need_reload = True

        threading.Thread.__init__(self)

    def run(self):
        try:
            while global_variable.thread_alive:
                partition_records = self._consumer.poll(timeout_ms=1000, max_records=30)
                for key, records in partition_records.items():
                    for record in records:
                        if record.value == "MONGO_EXTRACTOR_RELOAD_CONFIG":  # record.key == "MONGO_EXTRACTOR_RELOAD_CONFIG"

                            # 20180320： remove global_variable.schemaTables_set
                            # tables_set = self._db.load_schemaTables()
                            # global_variable.thread_lock.acquire()
                            # global_variable.schemaTables_set = tables_set
                            # global_variable.thread_lock.release()
                            # global_variable.need_reload = True
                            # LOG.info("mongo extractor read config: " + str(global_variable.schemaTables_set))

                            global_variable.need_reload = True
                            print "need_reload"

                        else:
                            LOG.info("skip CTRL Message : " + record.key)
            print "oplog thread send exit"

        except Exception as ex:
            global_variable.thread_alive = False
            raise ex

        finally:
            self._consumer.close(30)
