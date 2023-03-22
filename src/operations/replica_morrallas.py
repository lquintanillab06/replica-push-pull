import datetime
from src.services import (get_sucursal_local,insert_or_update_entity,create_replica_log,get_last_run_replica_log,get_entities,
                          get_replica_entity, insert_replica_entity)
from src.database import get_database_connections_pool



def replica_push_morrallas():
    sucursal = get_sucursal_local()

    if sucursal['nombre'] != 'OFICINAS':

        localDB, remoteDB = get_database_connections_pool()
        action = 'PUSH'
        fecha = datetime.datetime.today()
        print(f"Ejecutando el push del movimiento !!! morrallas")
        last_run = get_last_run_replica_log(remoteDB,fecha,'morralla',sucursal['nombre'],action) 
        print(f"Ultima corrida {last_run}")
        query = f"select * from morralla where (date_created >= '{last_run}' or last_updated >= '{last_run}' ) and sucursal_id = '{sucursal['id']}'"
        morrallas = get_entities(localDB,query)
        for morralla in morrallas:
            print(f"Morralla: {morralla['id']}")
            insert_or_update_entity(remoteDB,'morralla',morralla)

        #if morrallas:
            #create_replica_log(remoteDB,'PUSH',sucursal['nombre'],'morralla')

def replica_pull_morrallas():
    sucursal = get_sucursal_local()

    if sucursal['nombre'] == 'OFICINAS':

        localDB, remoteDB = get_database_connections_pool()
        action = 'PULL'
        fecha = datetime.datetime.today()
        print(f"Ejecutando el pull del movimiento !!! morrallas")
        last_run = get_last_run_replica_log(remoteDB,fecha,'morralla',sucursal['nombre'],action) 
        print(f"Ultima corrida {last_run}")
        query = f"select * from morralla where (date_created >= '{last_run}' or last_updated >= '{last_run}')"
        morrallas = get_entities(remoteDB,query)
        for morralla in morrallas:
            print(f"Morralla: {morralla['id']}")
            insert_or_update_entity(localDB,'morralla',morralla)

        #if morrallas:
            #create_replica_log(remoteDB,'PUSH',sucursal['nombre'],'morralla')

    else:
        print("No se puede ejecutar el pull de morrallas porque no esta en oficinas")
