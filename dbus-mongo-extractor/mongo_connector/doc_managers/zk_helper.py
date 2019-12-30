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


import logging

from kazoo.client import KazooClient
from kazoo.handlers.threading import KazooTimeoutError

from mongo_connector import constants

LOG = logging.getLogger(__name__)

__version__ = constants.__version__


class ZooKeeperHelper(object):
    _property_files = {"config.properties": None,
                       "producer.properties": None,
                       "consumer.properties": None}
    _auth = [("digest", "DBus:CHEOi@TSeyLfSact")]

    def __init__(self, url="localhost:2181", tid="defaultDB"):
        self._url = url
        self._extractor_root = constants.DEFAULT_ZK_DBUS_EXTRACTOR_ROOT + "/" + tid + constants.DEFAULT_ZK_MONGO_EXTRACTOR_PREFIX
        LOG.info("mongo extractor zk: %s, path: %s", self._url, self._extractor_root)
        pass

    def _make_properties(self, data):
        properties = {}
        try:
            lines = str(data)
            for line in lines.splitlines():
                line = line.strip().replace('\r', '')
                line = line.strip().replace('\n', '')
                if line.find("#") != -1:
                    linetrim = str(line[0:line.find('#')]).strip()
                    if len(linetrim) == 0:
                        # skip comment line
                        continue

                if line.find('=') > 0:
                    strs = line.split('=')
                    strs[1] = line[len(strs[0]) + 1:]
                    properties[strs[0].strip()] = strs[1].strip()
        except Exception, e:
            raise e
        return properties

    def load_properties(self):
        zk = None
        try:
            zk = KazooClient(hosts=self._url, auth_data=self._auth)
            zk.start()

            for file_name in self._property_files:
                path = self._extractor_root + "/" + file_name
                LOG.info("read zk node: %s", path)
                data, stat = zk.get(path)
                self._property_files[file_name] = self._make_properties(data)

            data, stat = zk.get(constants.DEFAULT_ZK_DBUSMGR_PATH)
            self._property_files["mysql.properties"] = self._make_properties(data)
        except KazooTimeoutError, e:
            LOG.error("read zk error! %s", str(e.message))
            raise e
        finally:
            if zk is not None:
                zk.stop()
                zk = None

    def get_properties(self, file_name):
        return self._property_files[file_name]


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    LOG = logging.getLogger(__name__)

    zk = ZooKeeperHelper("vdbus-7:2181", "mdb")
    zk.load_properties()
    print str(zk.get_properties("config.properties"))
    print str(zk.get_properties("mysql.properties"))

    # python2json = {}
    # listData = [1,2,3]
    # python2json["listData"] = listData
    # python2json["strData"] = "test python obj 2 json"
    # json_str = json.dumps(python2json)
    # print json_str
