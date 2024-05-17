from datetime import datetime
from src.database import get_local_pool_connection, get_remote_pool_connection

#import src.database

from  .replica_entity_service import pull_entity,push_entity


def replica_audit(table, action,audit_table, dispersar=False):
    try:
        localDB = get_local_pool_connection()
    except Exception as e:
        print(e)
    try:
        remoteDB = get_remote_pool_connection()
    except Exception as e:
        print(e)

    audits = get_audits(localDB,remoteDB,audit_table,action,table)
    for audit in audits:
        print(audit)
        if action == 'PULL':
            replicado, error = pull_entity(localDB,remoteDB,audit['table_name'],audit['persisted_object_id'],audit['event_name'])
            if replicado:
                actualizar_audit(remoteDB,audit_table,audit['id'],'Replicado')
            if error:
                actualizar_audit(remoteDB,audit_table,audit['id'],'Error')
        if action == 'PUSH':
            replicado, error = push_entity(localDB,remoteDB,audit['table_name'],audit['persisted_object_id'],audit['event_name'])
            if replicado:
                print("REPLICADO")
                print(f"Dispersar: {dispersar}")
                sucursales = []
                if dispersar:
                    sucursales = get_sucursales_replica(localDB) 
                if sucursales:
                    for sucursal in sucursales:
                        print("**********************************************")
                        print(sucursal)
                        print("**********************************************")
                        crear_audit(remoteDB,sucursal['server'], audit)
                else:
                    target = audit['target']
                    if target == 'CENTRAL':
                        target = 'OFICINAS'
                    crear_audit(remoteDB,target, audit)
                actualizar_audit(localDB,audit_table,audit['id'],'Replicado_cloud')
            if error:
                actualizar_audit(remoteDB,audit_table,audit['id'],'Error')
               
def get_audits(localDB,remoteDB,audit_table,action,table, last_run): 
    audits = []
    if action == 'PUSH':
        try:
            local_cnx = localDB.get_conexion()
            local_cursor = local_cnx.cursor(dictionary=True, buffered=True)
        except Exception as e:
            print(e)
        try:
            query_audit = f"select * from {audit_table} where table_name = '{table}' and replicated_cloud is null and date_created >= '{last_run}'  order by date_created"
            local_cursor.execute(query_audit)
            audits = local_cursor.fetchall()
            local_cnx.close()
        except Exception as e:
            print(e)
            local_cnx.close()
    if action == 'PULL':

        try:
            sucursal_local = get_sucursal_local()
        except Exception as e:
            print(e)

        try:
            remote_cnx =  remoteDB.get_conexion()
            remote_cursor = remote_cnx.cursor(dictionary=True, buffered=True)
        except Exception as e:
            print(e)

        try:
            
            query_audit = f"select * from audit_log where table_name = '{table}' and target = '{sucursal_local['nombre']}' and replicated_cloud is null and date_created >= '2023/02/21' order by date_created"        
            remote_cursor.execute(query_audit)
            audits = remote_cursor.fetchall()
            remote_cnx.close()
        except Exception as e:
            print(e)
            remote_cnx.close()
        
    return audits

def get_sucursales_replica(localDB):
    sucursales = [] 
    try:
        local_cnx = localDB.get_conexion()
        cursor = local_cnx.cursor(dictionary=True, buffered=True)
        sql_config_local ="select * from app_config"
        cursor.execute(sql_config_local)
        config_local= cursor.fetchone() 
        sql_sucursal_local = f"select * from sucursal where id = '{config_local['sucursal_id']}' " 
        cursor.execute(sql_sucursal_local)
        sucursal_local = cursor.fetchone()
        sql_sucursales = f"select * from data_source_replica where activa is true and server <> '{sucursal_local['nombre']}'"
        cursor.execute(sql_sucursales)
        sucursales = cursor.fetchall()
        local_cnx.close()
    except Exception as e:
        print(e)
        local_cnx.close()

    return sucursales 

def get_sucursal_local():   
    try:
        localDB = get_local_pool_connection()
        local_cnx = localDB.get_conexion()
        cursor = local_cnx.cursor(dictionary=True, buffered=True)
        sql_config_local ="select * from app_config"
        cursor.execute(sql_config_local)
        config_local= cursor.fetchone() 
        sql_sucursal_local = f"select * from sucursal where id = '{config_local['sucursal_id']}' " 
        cursor.execute(sql_sucursal_local)
        sucursal_local = cursor.fetchone()
        local_cnx.close()
        return sucursal_local
    except Exception as e:
        print(e)
        local_cnx.close()
        return None


def crear_audit(connectionDB,sucursal_name, audit_origen):
    try:
        cnx =  connectionDB.get_conexion()
        cursor = cnx.cursor(dictionary=True, buffered=True)
        audit = {
                    'version': audit_origen['version'],
                    'persisted_object_id': audit_origen['persisted_object_id'],
                    'target':sucursal_name,
                    'date_created': datetime.now(),
                    'last_updated': datetime.now(),
                    'name':audit_origen['name'],
                    'event_name': audit_origen['event_name'],
                    'table_name': audit_origen['table_name'],
                    'source': 'CENTRAL',
                }
        sql_insert_audit = """
                            insert into audit_log 
                            (version,persisted_object_id,target,date_created,last_updated,name,event_name,table_name,source)
                            values
                            (%(version)s,%(persisted_object_id)s,%(target)s,%(date_created)s,%(last_updated)s,%(name)s,%(event_name)s,%(table_name)s,%(source)s)
                            """
        
        cursor.execute(sql_insert_audit,audit)
        cnx.commit()
        cnx.close()
    except Exception as e:
        print(e)
        cnx.close()

def crear_audit_operacion(connectionDB,audit):
    try:
        cnx =  connectionDB.get_conexion()
        cursor = cnx.cursor(dictionary=True, buffered=True)
        sql_insert_audit = """
                            insert into audit_log 
                            (version,persisted_object_id,target,date_created,last_updated,name,event_name,table_name,source)
                            values
                            (%(version)s,%(persisted_object_id)s,%(target)s,%(date_created)s,%(last_updated)s,%(name)s,%(event_name)s,%(table_name)s,%(source)s)
                            """   
        cursor.execute(sql_insert_audit,audit)
        cnx.commit()
        cnx.close()
    except Exception as e:
        print(e)
        cnx.close()


def actualizar_audit(connectionDB,audit_table,audit_id,message):

    try:
        cnx =  connectionDB.get_conexion()
        cursor = cnx.cursor(dictionary=True, buffered=True)
    except Exception as e:
        print(e)

    try:
        sql_update =f"update {audit_table} set replicated_cloud = %(replicado)s, message = %(message)s where id = %(audit_id)s "
        cursor.execute(sql_update,{"replicado":datetime.now(), "message" : message,"audit_id":audit_id})
        cnx.commit()
        cnx.close()
    except Exception as  e:
        print(e)
        cnx.close()


def get_maestro_detalle(connectionDB,query_maestro,query_detalle):

    try:
        cnx =  connectionDB.get_conexion()
        cursor = cnx.cursor(dictionary=True, buffered=True)
    except Exception as e:
        print(e)

    maestro = None
    partidas = []
        
    try:
        cursor.execute(query_maestro)
        maestro  = cursor.fetchone()
        cursor.execute(query_detalle)
        partidas = cursor.fetchall()
        cnx.close()
    except Exception as e:
        print(e)
        cnx.close()

    return maestro, partidas


def get_entities(connectionDB,query):
    try:
        cnx =  connectionDB.get_conexion()
        cursor = cnx.cursor(dictionary=True, buffered=True)
    except Exception as e:
        print(e)

    entities = []
        
    try:
        cursor.execute(query)
        entities = cursor.fetchall()
        cnx.close()
    except Exception as e:
        print(e)
        cnx.close()

    return entities


def create_replica_log(remoteDB,action,sucursal,table):
    try:
        remote_cnx =  remoteDB.get_conexion()
        remote_cursor = remote_cnx.cursor(dictionary=True, buffered=True)
        audit = {
                    "version":0,
                    "date_created": datetime.today(),
                    "fecha_control":datetime.today(),
                    "last_run":datetime.today(),
                    "action":action,
                    "sucursal":sucursal,
                    "entity": table

                }
        sql_insert_replica = """
                            insert into replica_log 
                            (version,date_created,fecha_control,last_run,action,sucursal,entity)
                            values
                            (%(version)s,%(date_created)s,%(fecha_control)s,%(last_run)s,%(action)s,%(sucursal)s,%(entity)s)
                            """
        
        remote_cursor.execute(sql_insert_replica,audit)
        remote_cnx.commit()
        remote_cnx.close()
    except Exception as e:
        print(e)
        remote_cnx.close()

def get_last_run_replica_log(remoteDB,fecha,entity,sucursal,action):
    try:
        remote_cnx =  remoteDB.get_conexion()
        remote_cursor = remote_cnx.cursor(dictionary=True, buffered=True)
    except Exception as e:
        print(e)
    query = f"select max(fecha_control) as last_run from replica_log where date(date_created) = '{fecha.date()}' and entity= '{entity}' and sucursal = '{sucursal}' and action = '{action}'"

    remote_cursor.execute(query)
    last_replica_log = remote_cursor.fetchone()
    remote_cnx.close()
    if not last_replica_log['last_run']:
        return fecha

    return last_replica_log['last_run']

def obtener_diferencias(lista_a, lista_b):
    diferencias = []
    if len(lista_b) != 0:
        diferencias = [i for i in lista_a if i not in lista_b]
    else:
        diferencias = lista_a
    return diferencias

