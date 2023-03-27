import datetime
from src.services import (get_sucursal_local,insert_or_update_entity,create_replica_log,get_last_run_replica_log,get_entities,
                          get_replica_entity,get_replica_entity_by_field)
from src.database import get_database_connections_pool

def replica_push_proveedor_prod():
    sucursal = get_sucursal_local()
    if sucursal['nombre'] == 'OFICINAS':
        print("*"*50)
        print("Ejecutando la replica push de proveedor productos")
        print("*"*50)

        localDB, remoteDB = get_database_connections_pool()
        action = 'PUSH'
        fecha = datetime.datetime.today()
        print(f"Ejecutando el push de proveedor productos !!!")
        last_run = get_last_run_replica_log(remoteDB,fecha,'proveedor_producto',sucursal['nombre'],action) 
        print(f"Ultima corrida {last_run}")
        query = f"Select * from proveedor_producto where last_updated >= '{last_run}'"
        productos = get_entities(localDB,query)

        for producto in productos:
            print(producto)
            insert_or_update_entity(remoteDB,'proveedor_producto',producto)

        #create_replica_log(remoteDB,'PUSH',sucursal['nombre'],'proveedor_producto')
    else:
        print("No se puede hacer push por que no esta en oficinas")
        

def replica_pull_proveedor_prod():
    sucursal = get_sucursal_local()
    if sucursal['nombre'] != 'OFICINAS':
        print("*"*50)
        print("Ejecutando la replica pull de proveedor productos")
        print("*"*50)

        localDB, remoteDB = get_database_connections_pool()
        action = 'PULL'
        fecha = datetime.datetime.today()
        print(f"Ejecutando el pull de proveedor productos !!!")
        last_run = get_last_run_replica_log(remoteDB,fecha,'proveedor_producto',sucursal['nombre'],action) 
        print(f"Ultima corrida {last_run}")
        query = f"Select * from proveedor_producto where last_updated >= '{last_run}'"
        productos = get_entities(localDB,query)

        for producto in productos:
            print(producto)
            insert_or_update_entity(remoteDB,'proveedor_producto',producto)

        #create_replica_log(remoteDB,'PULL',sucursal['nombre'],'proveedor_producto')
    else:
        print("No se puede hacer pull por que esta en oficinas")