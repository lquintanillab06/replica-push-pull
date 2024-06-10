import datetime
from src.services import (get_maestro_detalle,get_replica_entity,get_entities,get_last_run_replica_log,get_sucursal_local,insert_or_update_entity,create_replica_log,
                            get_sucursales_replica)
from src.database import get_database_connections_pool


def replica_push_transformaciones():
    replica_push_movimientos('transformacion', 'transformacion_det')

def replica_push_movimientos(table, table_det):
    localDB, remoteDB = get_database_connections_pool()
    action = 'PUSH'
    fecha = datetime.datetime.today()
    sucursal = get_sucursal_local()
    replica_push_movimiento_inventario(localDB, remoteDB, action,table,table_det,fecha,sucursal)


def replica_push_movimiento_inventario(localDB, remoteDB, action,table,table_det,fecha,sucursal):
    print(f"Ejecutando el push del movimiento !!! {table}")
    last_run = get_last_run_replica_log(remoteDB,fecha,table,sucursal['nombre'],action) 
    print(f"Ultima corrida {last_run}")
    query = f"select * from {table} where (date_created >= '{last_run}' or last_updated >= '{last_run}' ) and sucursal_id = '{sucursal['id']}'"
    entities = get_entities(localDB,query)
    for entity in entities:
        mov,partidas = get_movimiento(localDB,table,table_det,entity['id'])
        print(f"Movimiento {mov['tipo']} : {mov['id']} - {mov['documento']} - {mov['fecha']}" )
        insert_or_update_entity(remoteDB,table,mov)
        for partida in partidas:
            print(f"Partida: {partida['id']}")
            inventario = get_replica_entity(localDB,'inventario',partida['inventario_id'])
            if inventario:
                print(f"Inventario: {inventario['id']}")
                insert_or_update_entity(remoteDB,'inventario',inventario)
            insert_or_update_entity(remoteDB,table_det,partida)
     
    # if entities:
    
    create_replica_log(remoteDB,action,sucursal['nombre'],table)

def replica_pull_movimientos(table, table_det):
    localDB, remoteDB = get_database_connections_pool()
    action = 'PULL'
    fecha = datetime.datetime.today()
    sucursal = get_sucursal_local()
    replica_pull_movimiento_inventario(localDB, remoteDB, action,table,table_det,fecha,sucursal)

def replica_pull_movimiento_inventario(localDB, remoteDB, action,table,table_det,fecha,sucursal):
    print(f"Ejecutando el pull del movimiento !!! {table}")
    if sucursal['nombre'] == 'OFICINAS':
        last_run = get_last_run_replica_log(remoteDB,fecha,table,sucursal['nombre'],action) 
        print(f"Ultima corrida {last_run}")
        query = f"select * from {table} where (date_created >= '{last_run}' or last_updated >= '{last_run}' )"
        print(query)
        entities = get_entities(remoteDB,query)
        for entity in entities:
            mov,partidas = get_movimiento(remoteDB,table,table_det,entity['id'])
            print(f"Movimiento {mov['tipo']} : {mov['id']} - {mov['documento']} - {mov['fecha']}" )
            insert_or_update_entity(localDB,table,mov)
            print(partidas)
            for partida in partidas:
                print(f"Partida: {partida['id']}")
                inventario = get_replica_entity(remoteDB,'inventario',partida['inventario_id'])
                if inventario:
                    print(f"Inventario: {inventario['id']}")
                    insert_or_update_entity(localDB,'inventario',inventario)
                insert_or_update_entity(localDB,table_det,partida)
        if entities:
            create_replica_log(remoteDB,action,sucursal['nombre'],table)
    else:
        print("No se puede hacer pull por que no esta en oficinas")


def get_movimiento(connectionDB,table,table_det,id):
    query_maestro = f"select * from {table} where id = '{id}' " 
    query_partidas = f"select * from {table_det} where {table}_id = '{id}' " 
    print(query_maestro)
    print(query_partidas)
    movimiento, partidas =  get_maestro_detalle(connectionDB,query_maestro,query_partidas)
    return movimiento, partidas 