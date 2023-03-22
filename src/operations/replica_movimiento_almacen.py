import datetime
from src.services import (get_maestro_detalle,get_replica_entity,get_entities,get_last_run_replica_log,get_sucursal_local,insert_or_update_entity,create_replica_log,
                            get_sucursales_replica)
from src.database import get_local_pool_connection,get_remote_pool_connection



def replica_movimiento_almacen(action):

    try:
        localDB = get_local_pool_connection()
    except Exception as e:
        print(e)  
    sucursal = get_sucursal_local()
    replica_movimiento_almacen_sucursal(action,sucursal)


def replica_sucursal_pull_movimiento_almacen():
    action = 'PULL'
    try:
        localDB = get_local_pool_connection()
    except Exception as e:
        print(e)
    sucursales = get_sucursales_replica(localDB)
    cnx = localDB.get_conexion()
    cursor = cnx.cursor(dictionary=True, buffered=True)
    for sucursal_replica in sucursales:
       
        sql_sucursal = f"select * from sucursal where nombre = '{sucursal_replica['server']}'  "
        cursor.execute(sql_sucursal)
        sucursal = cursor.fetchone()
       
        print(sucursal)
        print("*"*100)
        print(sucursal_replica['server'])
        replica_movimiento_almacen_sucursal(action,sucursal)
     
    cnx.close()

   

def replica_movimiento_almacen_sucursal(action,sucursal):
    try:
        localDB = get_local_pool_connection()
    except Exception as e:
        print(e)
    try:
        remoteDB = get_remote_pool_connection()
    except Exception as e:
        print(e) 
    table = 'movimiento_de_almacen'
    fecha = datetime.datetime.today()
    if action == 'PUSH':
        replica_push_movimiento_almacen(localDB, remoteDB, action,table,fecha,sucursal)
    if action == 'PULL':
        replica_pull_movimiento_almacen(localDB, remoteDB, action,table,fecha,sucursal)



def replica_pull_movimiento_almacen(localDB, remoteDB, action,table,fecha,sucursal):

    last_run = get_last_run_replica_log(remoteDB,fecha,table,sucursal['nombre'],action) 
    query = f"select * from {table} where (date_created >= '{last_run}' or last_updated >= '{last_run}' ) and sucursal_id = '{sucursal['id']}'"
    entities = get_entities(remoteDB,query)
    #create_replica_log(remoteDB,action,sucursal['nombre'],table)
    for entity in entities:
        mov,partidas = get_movimiento(remoteDB,entity['id'])
        insert_or_update_entity(localDB,table,mov)
        for partida in partidas:
            inventario = get_replica_entity(remoteDB,'inventario',partida['inventario_id'])
            insert_or_update_entity(localDB,'inventario',inventario)
            insert_or_update_entity(localDB,'movimiento_de_almacen_det',partida)


def replica_push_movimiento_almacen(localDB, remoteDB, action,table,fecha,sucursal):

    last_run = get_last_run_replica_log(remoteDB,fecha,table,sucursal['nombre'],action) 
    query = f"select * from {table} where (date_created >= '{last_run}' or last_updated >= '{last_run}' ) and sucursal_id = '{sucursal['id']}'"
    entities = get_entities(localDB,query)
    #create_replica_log(remoteDB,action,sucursal['nombre'],table)
    for entity in entities:
        mov,partidas = get_movimiento(localDB,entity['id'])
        insert_or_update_entity(remoteDB,table,mov)
        for partida in partidas:
            inventario = get_replica_entity(localDB,'inventario',partida['inventario_id'])
            insert_or_update_entity(remoteDB,'inventario',inventario)
            insert_or_update_entity(remoteDB,'movimiento_de_almacen_det',partida)



def get_movimiento(localDB,id):
    query_sol = f"select * from movimiento_de_almacen where id = '{id}' " 
    query_partidas = f"select * from movimiento_de_almacen_det where movimiento_de_almacen_id = '{id}' " 
    movimiento, partidas =  get_maestro_detalle(localDB,query_sol,query_partidas)
    return movimiento, partidas 