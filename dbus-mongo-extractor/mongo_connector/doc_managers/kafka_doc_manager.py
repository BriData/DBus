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

"""Receives documents from the oplog worker threads and indexes them
    into the backend.

    This file is a document manager for MongoDB, but the intent
    is that this file can be used as an example to add on different backends.
    To extend this to other systems, simply implement the exact same class and
    replace the method definitions with API calls for the desired backend.
    """

import logging
import pymongo
import json
import time
import global_variable
from datetime import date, datetime

from zk_helper import ZooKeeperHelper
from mysqldb_helper import MysqlDBHelper
from kafka_client_creator import KafkaClientCreator
from kafka_ctrl_topic_reader import KafkaCtrlTopicReader

from mongo_connector import errors, constants
from mongo_connector.util import exception_wrapper
from mongo_connector.doc_managers.doc_manager_base import DocManagerBase

wrap_exceptions = exception_wrapper({
    pymongo.errors.ConnectionFailure: errors.ConnectionFailed,
    pymongo.errors.OperationFailure: errors.OperationFailed})

LOG = logging.getLogger(__name__)

__version__ = constants.__version__
"""MongoDB DocManager version information

This is packaged with mongo-connector so it shares the same version.
Downstream DocManager implementations should add their package __version__
string here, for example:

__version__ = '0.1.0'
"""


class MyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, date):
            return obj.strftime('%Y-%m-%d')
        else:
            return json.JSONEncoder.default(self, obj)


class DocManager(DocManagerBase):
    """The DocManager class creates a connection to the backend engine and
        adds/removes documents, and in the case of rollback, searches for them.

        The reason for storing id/doc pairs as opposed to doc's is so that
        multiple updates to the same doc reflect the most up to date version as
        opposed to multiple, slightly different versions of a doc.

        We are using MongoDB native fields for _id and ns, but we also store
        them as fields in the document, due to compatibility issues.
        """
    _ds_name = None
    _db = None
    _producer = None
    _cache_schemaTables_set = set()

    def __init__(self, url, tid, **kwargs):
        """ Verify URL and establish a connection.
        """
        try:
            zk_url = url
            tid = tid
            # tid = "mongos_test"

            zk = ZooKeeperHelper(zk_url, tid)
            zk.load_properties()
            LOG.info("load properties from ZK.")

            # prepare db object
            self._ds_name = zk.get_properties("config.properties").get("database.name")
            mysql_props = zk.get_properties("mysql.properties")
            self._db = MysqlDBHelper(self._ds_name, mysql_props)
            self._db.load_datasource_info()

            # start a thread to read control topic and receive reload event.
            consumer_props = zk.get_properties("consumer.properties")
            reader = KafkaCtrlTopicReader(self._db, consumer_props)
            reader.start()
            # waiting reader thread start and read schema_table set
            self.wait_for_initial_reload()

            # create kafka producer
            kafka = KafkaClientCreator(self._ds_name)
            producer_props = zk.get_properties("producer.properties")
            self._producer = kafka.create_producer(producer_props)

            # d_bus
            self.message_status_queue = global_variable.message_status_queue

        except Exception as ex:
            raise ex

    # waiting reader thread start and read schema_table set
    def wait_for_initial_reload(self):
        # sleep 30 sec
        for i in range(1, 10):
            if not global_variable.thread_alive:
                LOG.error("oplog thread exit...")
                raise Exception("oplog thread exit")

            time.sleep(3)
            LOG.info("wait init schema table set...")

            if self.load_schema_tables():
                break

        if len(self._cache_schemaTables_set) == 0:
            LOG.error("Cann't read tables from dbusmgr.")
            raise Exception("Cann't read tables from dbusmgr.")

    # load schema_tables from db,return success or fail
    def load_schema_tables(self):
        # 20180320： remove global_variable.schemaTables_set
        # global_variable.thread_lock.acquire()
        # if len(global_variable.schemaTables_set) > 0:
        #     self._cache_schemaTables_set = global_variable.schemaTables_set.copy()
        # global_variable.thread_lock.release()

        print "load_schemaTables"
        self._cache_schemaTables_set = self._db.load_schemaTables()
        global_variable.need_reload = False
        for item in self._cache_schemaTables_set:
            print item

        if len(self._cache_schemaTables_set) > 0:
            LOG.info("reload schema tables: " + str(self._cache_schemaTables_set))
            return True
        else:
            return False

    @staticmethod
    def _db_and_collection(namespace):
        return namespace.split('.', 1)

    @staticmethod
    def _get_meta_collection(namespace):
        return namespace

    @wrap_exceptions
    def _meta_collections(self):
        """Provides the meta collections currently being used
        """
        if self.use_single_meta_collection:
            yield self.meta_collection_name
        else:
            for name in self.meta_database.collection_names(
                    include_system_collections=False):
                yield name

    def stop(self):
        """Stops any running threads
        """
        global_variable.thread_alive = False

        if self._producer is not None:
            self._producer.close()
            self._producer = None

        for i in range(0, 3):
            time.sleep(1)
            LOG.info("waiting for exit ...")

        LOG.info(
            "Kafka DocManager Stopped: If you will not target this system "
            "again with mongo-connector then you may drop the database "
            "__mongo_connector, which holds metadata for Mongo Connector."
        )

    ###########################################################
    # change message status queue
    def append_or_update_element(self, timestamp):
        with self.message_status_queue as message_status:
            message_status.append_or_update(timestamp)

    ##################################################################################

    # handle DDL command
    # handle command just can know namespace's database, so filter just can filter database, can't filter tables
    # def handle_command(self, doc, namespace, timestamp):
    @wrap_exceptions
    def handle_command(self, entry, namespace, timestamp):
        db, _ = self._db_and_collection(namespace)

        def filter_db(database):
            flag = False
            for ds_name in self._cache_schemaTables_set:
                dbs, _ = self._db_and_collection(ds_name)
                if dbs == database:
                    flag = True
                    break
            return flag

        def filter_collection(database):
            if database not in self._cache_schemaTables_set:
                print "skip update : " + namespace
                return False

        doc = entry.get('o')
        message_entry = {}

        if doc.get('dropDatabase'):
            message_entry['_ns'] = namespace
            if filter_db(db) is False:
                print "skip drop : " + db
                return
            print "drop database: " + str(db)
        # rename collection's namespace is to or from ?
        elif doc.get('renameCollection'):
            from_ns = doc['renameCollection']
            to_ns = doc['to']
            message_entry['_ns'] = to_ns
            if filter_collection(from_ns) is False:
                print "skip rename collection : " + from_ns
                return
            print "rename collection: " + str(message_entry['_ns']) + " to " + str(to_ns)
        # create collection (when create database, oplog.rs don't produce any information)
        elif doc.get('create'):
            # if doc.get('ns'):  # handle create collection operation
            message_entry['_ns'] = doc.get('idIndex')['ns']
            if filter_collection(message_entry['_ns']) is False:
                print "skip create collection : " + str(message_entry['_ns'])
                return
            print "create collection: " + str(message_entry['_ns'])
            """else:
                print "skip create index"
                return
            """
        # drop collection
        elif doc.get('drop'):
            message_entry['_ns'] = db + "." + doc['drop']
            if filter_collection(message_entry['_ns']) is False:
                print "skip drop collection : " + message_entry['_ns']
                return
            print "drop collection: " + str(message_entry['_ns'])
        else:
            print "unknown Command:" + db

        message_entry['_ts'] = timestamp
        message_entry['_op'] = entry['op']
        message_entry['_h'] = entry['h']
        message_entry['_t'] = entry['t']
        message_entry['_o'] = entry.get('o')
        message_entry['_v'] = entry.get('v')

        try:
            message = json.dumps(message_entry, cls=MyEncoder)
        except TypeError, e:
            print "TypeError: " + e.message
        else:
            self.append_or_update_element(timestamp)

            future = self._producer.send(self._db.get_topic(), value=message, key="")

            future.add_callback(self.callback, timestamp)
            future.add_errback(self.callback_fail, timestamp)

    @wrap_exceptions
    def update(self, entry, namespace, timestamp):
        """Apply updates given in update_spec to the document whose id
        matches that of doc.
        update collections unset or set some columns update() $set
        """
        if namespace not in self._cache_schemaTables_set:
            print "skip update : " + namespace
            return
        database, coll = self._db_and_collection(namespace)
        print "update: " + database + "." + coll

        key = str(entry['o2']['_id'])
        message_entry = dict()
        message_entry['_id'] = key
        message_entry['_ns'] = namespace
        message_entry['_ts'] = timestamp
        message_entry['_op'] = entry['op']
        message_entry['_h'] = entry['h']
        message_entry['_t'] = entry['t']
        message_entry['_o'] = entry.get('o')
        message_entry['_v'] = entry['v']
        try:
            message = json.dumps(message_entry, cls=MyEncoder)
        except TypeError, e:
            print "TypeError: " + e.message
        else:
            print "update: " + str({"id_field": key, "ns": namespace}) \
                  + " , " + str({"id_field": key, "_ts": timestamp, "ns": namespace})
            self.append_or_update_element(timestamp)
            future = self._producer.send(self._db.get_topic(), value=message, key="")
            future.add_callback(self.callback, timestamp)
            future.add_errback(self.callback_fail, timestamp)

    @wrap_exceptions
    def insert(self, entry, namespace, timestamp):
        """insert a document into Mongo
        """
        if namespace not in self._cache_schemaTables_set:
            print "skip upsert : " + namespace
            return
        database, coll = self._db_and_collection(namespace)
        # print "upsert: " + database + "." + coll

        key = str(entry['o']['_id'])
        message_entry = dict()
        message_entry['_id'] = key
        message_entry['_ns'] = namespace
        message_entry['_ts'] = timestamp
        message_entry['_op'] = entry['op']
        message_entry['_h'] = entry['h']
        message_entry['_t'] = entry['t']
        message_entry['_v'] = entry['v']
        doc = entry.get('o')
        doc.pop('_id')
        message_entry['_o'] = doc
        try:
            print "id" + doc.get('id') + "  : username:" + doc.get('username')
        except BaseException:
            print doc

        try:
            message = json.dumps(message_entry, cls=MyEncoder)
        except TypeError, e:
            print "TypeError: " + e.message
        else:
            self.append_or_update_element(timestamp)
            future = self._producer.send(self._db.get_topic(), value=message, key="")
            future.add_callback(self.callback, timestamp)
            future.add_errback(self.callback_fail, timestamp)

    @wrap_exceptions
    def bulk_upsert(self, docs, namespace, timestamp):
        LOG.error("bulk_upsert is not supported")
        raise Exception("bulk_upsert is not supported")

    # @wrap_exceptions
    # def bulk_upsert(self, docs, namespace, timestamp):
    #     def iterate_chunks():
    #         dbname, collname = self._db_and_collection(namespace)
    #         collection = self.mongo[dbname][collname]
    #         meta_collection_name = self._get_meta_collection(namespace)
    #         meta_collection = self.meta_database[meta_collection_name]
    #         more_chunks = True
    #         while more_chunks:
    #             bulk = collection.initialize_ordered_bulk_op()
    #             bulk_meta = meta_collection.initialize_ordered_bulk_op()
    #             for i in range(self.chunk_size):
    #                 try:
    #                     doc = next(docs)
    #                     selector = {'_id': doc['_id']}
    #                     bulk.find(selector).upsert().replace_one(doc)
    #                     meta_selector = {self.id_field: doc['_id']}
    #                     bulk_meta.find(meta_selector).upsert().replace_one({
    #                         self.id_field: doc['_id'],
    #                         'ns': namespace,
    #                         '_ts': timestamp
    #                     })
    #                 except StopIteration:
    #                     more_chunks = False
    #                     if i > 0:
    #                         yield bulk, bulk_meta
    #                     break
    #             if more_chunks:
    #                 yield bulk, bulk_meta
    #
    #     for bulk_op, meta_bulk_op in iterate_chunks():
    #         try:
    #             bulk_op.execute()
    #             meta_bulk_op.execute()
    #         except pymongo.errors.DuplicateKeyError as e:
    #             LOG.warn('Continuing after DuplicateKeyError: '
    #                      + str(e))
    #         except pymongo.errors.BulkWriteError as bwe:
    #             LOG.error(bwe.details)
    #             raise e

    @wrap_exceptions
    def remove(self, entry, namespace, timestamp):
        """Removes document from Mongo
        The input is a python dictionary that represents a mongo document.
        The documents has ns and _ts fields.
        """
        if namespace not in self._cache_schemaTables_set:
            print "skip remove : " + namespace
            return
        database, coll = self._db_and_collection(namespace)
        print "remove: " + database + "." + coll
        key = str(entry['o']['_id'])
        message_entry = dict()
        message_entry['_id'] = key
        message_entry['_ns'] = namespace
        message_entry['_ts'] = timestamp
        message_entry['_op'] = entry['op']
        message_entry['_h'] = entry['h']
        message_entry['_t'] = entry['t']
        message_entry['_v'] = entry['v']
        '''doc = entry.get('o')
        doc.pop('_id')
        message_entry['_o'] = doc
        '''
        try:
            message = json.dumps(message_entry, cls=MyEncoder)
        except TypeError, e:
            print "TypeError: " + e.message
        else:
            self.append_or_update_element(timestamp)

            future = self._producer.send(self._db.get_topic(), value=message, key="")

            future.add_callback(self.callback, timestamp)
            future.add_errback(self.callback_fail, timestamp)

    @wrap_exceptions
    def insert_file(self, f, namespace, timestamp):
        LOG.error("insert_file is not supported")
        raise Exception("insert_file is not supported")

    # @wrap_exceptions
    # def insert_file(self, f, namespace, timestamp):
    #     database, coll = self._db_and_collection(namespace)
    #
    #     id = GridFS(self.mongo[database], coll).put(f, filename=f.filename)
    #     print "insert_file: " + str({self.id_field: f._id, "ns": namespace}) \
    #           + " , " + str({self.id_field: f._id, '_ts': timestamp, 'ns': namespace, 'gridfs_id': id})

    # unkown ， 20180320
    @wrap_exceptions
    def search(self, start_ts, end_ts):
        """Called to query Mongo for documents in a time range.
        """
        for meta_collection_name in self._meta_collections():
            meta_coll = self.meta_database[meta_collection_name]
            for ts_ns_doc in meta_coll.find(
                    {'_ts': {'$lte': end_ts, '$gte': start_ts}}):
                yield ts_ns_doc

    def commit(self):
        """ Performs a commit
        """
        return

    @wrap_exceptions
    def get_last_doc(self):
        LOG.error("get_last_doc is not supported")
        raise Exception("get_last_doc is not supported")

    # @wrap_exceptions
    # def get_last_doc(self):
    #     """Returns the last document stored in Mongo.
    #     """
    #
    #     def docs_by_ts():
    #         for meta_collection_name in self._meta_collections():
    #             meta_coll = self.meta_database[meta_collection_name]
    #             for ts_ns_doc in meta_coll.find(limit=-1).sort('_ts', -1):
    #                 yield ts_ns_doc
    #
    #     return max(docs_by_ts(), key=lambda x: x["_ts"])

    ######################################################################
    # define callback and error callback function
    # when send success, set global variate sendstatus_locking_dict, key is timestamp is_done to true
    def callback(self, timestamp, meta):
        """with self.sendstatus_locking_dict as send_status:
            send_status_dict = send_status.get_dict()
            if timestamp in send_status_dict:
                send_status_dict[timestamp] = {"is_sended": True, "is_done": True}
                LOG.info("send is done, the information is " + str(send_status_dict[timestamp]))
                print "have send success."
        """
        with self.message_status_queue as message_status:
            element = message_status.get_queue_element(timestamp)
            if element is not None:
                element.ok()

            LOG.info("send is done, the information is " + str(meta))
            LOG.debug("have send success")
            print "have send success."

    # when failed to send, then seek
    def callback_fail(self, timestamp, meta):
        with self.message_status_queue as message_status:
            element = message_status.get_queue_element(timestamp)
            if element is not None:
                if not element.is_ok():
                    element.fail()
                else:
                    LOG.info("the element has been sended" + meta)

                # 貌似没有用吧
                # message_status.fail_flag = True

            LOG.info("send have failed, the information is " + meta)
            print "send failed"


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    LOG = logging.getLogger(__name__)

    zk_url_test = "vdbus-7:2181"

    manager = DocManager(zk_url_test, "mongos_test2")
    manager.stop()

    #
    # kafka = KafkaClientCreator(ds_name)
    # producer_props = zk.get_properties("producer.properties")
    # producer = kafka.create_producer(producer_props)
    #
    # producer.close(30)
    #
