import datetime
from src.services import (get_sucursal_local,insert_or_update_entity,create_replica_log,get_last_run_replica_log,get_entities,
                          get_replica_entity, insert_replica_entity)
from src.database import get_database_connections_pool




def replica_requisicion_de_material():

    sucursal = get_sucursal_local()

    if sucursal['nombre'] != 'OFICINAS':
        localDB, remoteDB = get_database_connections_pool()
        action = 'PUSH'
        fecha = datetime.datetime.today()
        print(f"Ejecutando el push del movimiento !!! requisiciones de material")
        last_run = get_last_run_replica_log(remoteDB,fecha,'requisicion_de_material',sucursal['nombre'],action) 
        print(f"Ultima corrida {last_run}")
        query = f"Select * from requisicion_de_material where cerrada is not null and last_updated >= '{last_run}'"
        requisiciones = get_entities(localDB,query)

        for requisicion in requisiciones:
            print(requisicion)

    create_replica_log(remoteDB,'PUSH',sucursal['nombre'],'requisicion_de_material')

