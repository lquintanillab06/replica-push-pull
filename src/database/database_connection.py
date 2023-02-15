import mysql.connector
import mysql.connector.pooling
import json


_local_connection = None
_remote_connection = None
_local_pool_connection = None
_remote_pool_connection = None

def get_local_connection():
    local_connection = DataBaseConnection("local_datasource")
    return local_connection 

def get_remote_connection():
    remote_connection = DataBaseConnection("remote_datasource")
    return remote_connection 

def get_local_pool_connection():
    global _local_pool_connection 
    if not _local_pool_connection :
        _local_pool_connection = DataBasePoolConnection("local_datasource")
    return _local_pool_connection 

def get_remote_pool_connection():
    global _remote_pool_connection 
    if not _remote_pool_connection :
        _remote_pool_connection = DataBasePoolConnection("remote_datasource")
    return _remote_pool_connection 

def get_persisted_local_connection():
    global _local_connection 
    if not _local_connection :
        _local_connection = DataBaseConnection("local_datasource")
    return _local_connection 

def get_persisted_remote_connection():
    global _remote_connection 
    if not _remote_connection :
        _remote_connection = DataBaseConnection("remote_datasource")
    return _remote_connection  

class DataBaseConnection():

    def __init__(self, datasource) :
        with open("conf/config.json") as f:
            _configuracion = json.loads(f.read())
            _datasource = _configuracion[datasource]

            self._cnx = mysql.connector.connect(
                    host = _datasource['host'],
                    user = _datasource['user'],
                    passwd = _datasource['passwd'] ,
                    database = _datasource['database'],
                    charset='utf8'
                )
            self._host = _datasource['host']
            self._database = _datasource['database']
            self._cursor = self._cnx.cursor(dictionary=True, buffered=True)

    @property
    def host(self):
        return self._host

    @property
    def database(self):
        return self._database

    @property
    def cursor(self):
        return self._cursor

    @property
    def conexion(self):
        return self._cnx
        

class DataBasePoolConnection():
    
    def __init__(self, datasource) :
        with open("conf/config.json") as f:
            _configuracion = json.loads(f.read())
            _datasource = _configuracion[datasource]

            self._cnx = mysql.connector.pooling.MySQLConnectionPool(
                    pool_name = "my_pool",
                    pool_size = 5,
                    pool_reset_session=True,
                    host = _datasource['host'],
                    user = _datasource['user'],
                    passwd = _datasource['passwd'] ,
                    database = _datasource['database'],
                    charset='utf8'
                )
            self._host = _datasource['host']
            self._database = _datasource['database']

    @property
    def host(self):
        return self._host

    @property
    def database(self):
        return self._database

    @property
    def conexion(self):
        return self._cnx

    def get_conexion(self):
        return self._cnx.get_connection()
