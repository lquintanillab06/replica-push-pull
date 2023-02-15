import datetime
from src.services import (get_audits, get_maestro_detalle, pull_entity, push_entity,actualizar_audit,crear_audit,get_replica_entity,
                          get_entities,get_last_run_replica_log,get_sucursal_local,create_replica_log)
from src.database import get_local_pool_connection,get_remote_pool_connection


def replica_inventario(action):
    try:
        localDB = get_local_pool_connection()
    except Exception as e:
        print(e)
    try:
        remoteDB = get_remote_pool_connection()
    except Exception as e:
        print(e) 
        
    table = 'inventario'
    fecha = datetime.datetime.today()
    sucursal = get_sucursal_local(localDB)
    print(sucursal)

    if action == 'PUSH':
        replica_push_inventario(localDB,remoteDB,action,table,fecha,sucursal)
    if action == 'PULL':
        replica_push_inventario(localDB,remoteDB,action,table,fecha,sucursal)



def replica_pull_inventario(localDB,remoteDB,action,table,fecha,sucursal):
    last_run = get_last_run_replica_log(remoteDB,fecha,table,sucursal['nombre'],action) 
    print(last_run)
    query = f"select * from {table} where date_created >= '{last_run}'  and sucursal_id = '{sucursal['id']}'"
    entities = get_entities(remoteDB,query)
    print(entities)
    #create_replica_log(remoteDB,action,sucursal['nombre'],table)
    print(len(entities))


def replica_push_inventario(localDB,remoteDB,action,table,fecha,sucursal):
    last_run = get_last_run_replica_log(remoteDB,fecha,table,sucursal['nombre'],action) 
    print(last_run)
    query = f"select * from {table} where date_created >= '{last_run}'  and sucursal_id = '{sucursal['id']}'"
    entities = get_entities(localDB,query)
    print(entities)
    #create_replica_log(remoteDB,action,sucursal['nombre'],table)
    print(len(entities))