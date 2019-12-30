import logging
import MySQLdb

from zk_helper import ZooKeeperHelper

LOG = logging.getLogger(__name__)


class MysqlDBHelper(object):
    def __init__(self, dsName="mongos_test2", mysql_props=None):
        self._ds_name = str(dsName)
        self._config = self._properties_to_config(mysql_props)
        pass

    def _properties_to_config(self, mysql_props):
        config = {
            'host': 'localhost',
            'port': 3306,
            'user': 'dbus',
            'passwd': 'dbus!@#123',
            'db': 'dbus',
            'charset': 'utf8'
        }

        if mysql_props is None:
            return config

        conn_str = str(mysql_props.get("url"))
        conn_strs = conn_str.split("/")
        if len(conn_strs) != 4:
            LOG.error("split mysql connection string error! %s", conn_str)
            return None

        host_part = conn_strs[2]
        host = host_part.split(":")[0]
        port = host_part.split(":")[1]

        db_part = conn_strs[3]
        db = db_part.split("?")[0]

        user = str(mysql_props.get("username"))
        passwd = str(mysql_props.get("password"))

        config.__setitem__("host", host)
        config.__setitem__("port", int(port))
        config.__setitem__("db", db)
        config.__setitem__("user", user)
        config.__setitem__("passwd", passwd)

        return config

    def load_datasource_info(self):
        conn = None
        try:
            config = self._config
            conn = MySQLdb.connect(**config)
            conn.autocommit(True)

            c = conn.cursor()

            print("dsname: %s" % (self._ds_name))
            # get dsId,
            dsid_sql = """ select id, topic, ctrl_topic from t_dbus_datasource where ds_name = %s and `status` = "active" """
            param = (self._ds_name,)
            c.execute(dsid_sql, param)
            ds_row = c.fetchone()
            if ds_row is not None:
                self._ds_id = ds_row[0]
                self._topic = ds_row[1]
                self._ctrl_topic = ds_row[2]
            else:
                raise Exception("datasource is not found. datasource name : " + self._ds_name)
                return schemaTables

        except Exception, e:
            LOG.error("read Mysql error! %s", str(e.message))
            raise e
        finally:
            if conn is not None:
                conn.close()

    def get_datasource_name(self):
        return self._ds_name

    def get_topic(self):
        return self._topic

    def get_ctrl_topic(self):
        return self._ctrl_topic

    def load_schemaTables(self):
        self.load_datasource_info()

        schemaTables = set()
        conn = None
        try:
            config = self._config
            conn = MySQLdb.connect(**config)
            conn.autocommit(True)
            c = conn.cursor()

            # schematable_sql = """ select `schema_name`, `table_name` from t_data_tables where ds_id = %s and `status` != 'inactive'  """

            # schematable_sql = """ select `tds.schema_name`, `tdt.table_name` from t_dbus_datasource tdd, t_data_schema tds, t_data_tables tdt where `tdd.id` = `tds.ds_id`  and `tdd.id` = %s and `tdd.status` = 'active' and `tds.status` = 'active'  and `tds.id` = `tdt.schema_id`  and `tdt.status` != 'inactive' """

            schematable_sql = """select tds.schema_name, tdt.table_name from t_dbus_datasource tdd, t_data_schema tds, t_data_tables tdt where tdd.id = tds.ds_id  and tdd.id = %s and tdd.status = 'active' and tds.status = 'active' and tds.id = tdt.schema_id and tdt.status != 'inactive'"""

            print("dsid :%s  sql %s" % (self._ds_id, schematable_sql))

            param = (self._ds_id,)
            c.execute(schematable_sql, param)
            all_rows = c.fetchall()
            for row in all_rows:
                schema = row[0]
                table = row[1]
                schemaTables.add(schema + "." + table)

            return schemaTables

        except Exception, e:
            LOG.error("read Mysql error! %s", str(e.message))
            raise e
        finally:
            if conn is not None:
                conn.close()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    LOG = logging.getLogger(__name__)

    # read param from zk
    zk = ZooKeeperHelper("vdbus-7:2181", "mdb")
    zk.load_properties()

    config_props = zk.get_properties("config.properties")
    ds_name = config_props.get("database.name")
    mysql_props = zk.get_properties("mysql.properties")

    db = MysqlDBHelper(ds_name, mysql_props)
    schemaTables_list = db.load_schemaTables()
    print schemaTables_list

    # mysql db testing
    # db = MysqlDBHelper("mdb")
    # db.load_schemaTables()

    # python2json = {}
    # listData = [1,2,3]
    # python2json["listData"] = listData
    # python2json["strData"] = "test python obj 2 json"
    # json_str = json.dumps(python2json)
    # print json_str
