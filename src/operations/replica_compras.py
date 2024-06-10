import datetime
from src.services import (get_sucursal_local,insert_or_update_entity,create_replica_log,get_last_run_replica_log,get_entities,
                          get_replica_entity,get_replica_entity_by_field, insert_replica_entity)
from src.database import get_database_connections_pool





def replica_push_compras():

    print("*"*50)
    print("Ejecutando la replica push de compras")
    print("*"*50)

    sucursal = get_sucursal_local()

    localDB, remoteDB = get_database_connections_pool()
    action = 'PUSH'
    fecha = datetime.datetime.today()
    print(f"Ejecutando el push de compras !!!")
    last_run = get_last_run_replica_log(remoteDB,fecha,'compra',sucursal['nombre'],action) 
    print(f"Ultima corrida {last_run}")
    query = f"Select * from compra where last_updated >= '{last_run}'"
    compras = get_entities(localDB,query)

    for compra in compras:
        print(compra['id'])
        compra_cloud = get_replica_entity(remoteDB,'compra',compra['id'])
        query_compra_det = f"select * from compra_det where compra_id = '{compra['id']}'"
        if compra_cloud:
            print(f"Compra cloud: {compra_cloud['id']}")
            print(f"Compra last updated: {compra['last_updated']}")
            print(f"Compra Cloud last updated: {compra_cloud['last_updated']}")
            
            if compra['last_updated'] > compra_cloud['last_updated'] :
                print(compra['last_updated'] > compra_cloud['last_updated'] )
                print("Ejecutar el PUSH")
                compras_det = get_entities(localDB,query_compra_det)
                insert_or_update_entity(remoteDB,'compra',compra)
                for compra_det in compras_det:
                    insert_or_update_entity(remoteDB,'compra_det',compra_det)
                    print(compra_det)
        else:
            print(f"Compra cloud: {compra_cloud}")
            print("La compra no existe ejecutar el PUSH")
            compras_det = get_entities(localDB,query_compra_det)
            insert_or_update_entity(remoteDB,'compra',compra)
            for compra_det in compras_det:
                insert_or_update_entity(remoteDB,'compra_det',compra_det)
                print(compra_det)
            

    create_replica_log(remoteDB,'PUSH',sucursal['nombre'],'compra')


def replica_pull_compras():
    print("*"*50)
    print("Ejecutando la replica pull de compras")
    print("*"*50)
    sucursal = get_sucursal_local()
    localDB, remoteDB = get_database_connections_pool()
    action = 'PULL'
    fecha = datetime.datetime.today()
    print(f"Ejecutando el PULL de compras !!!")
    last_run = get_last_run_replica_log(remoteDB,fecha,'compra',sucursal['nombre'],action) 
    print(f"Ultima corrida {last_run}")


    query = ""
    print(sucursal['nombre'] )
    if sucursal['nombre'] == 'OFICINAS':
        query = f"Select * from compra where last_updated >= '{last_run}'"
    else:
        query = f"Select * from compra where last_updated >= '{last_run}' and sucursal_id in ('{sucursal['id']}', '402880fc5e4ec411015e4ec64161012c')"
    print(query)
    compras = get_entities(remoteDB,query)
    #print(compras)
    for compra in compras:
        print(compra['id'],compra['sucursal_id'])
        compra_local = get_replica_entity(localDB,'compra',compra['id'])
        compra_det = get_replica_entity_by_field(remoteDB,'compra_det','compra_id',compra['id'])
        print(f"Compra Det: {compra_det['id']}")
        if compra_local:
            print(f"Compra local: {compra_local['id']}")
            print(f"Compra  last updated: {compra['last_updated']}")
            print(f"Compra local last updated: {compra_local['last_updated']}")
            
            if compra['last_updated'] > compra_local['last_updated'] :
                print(compra['last_updated'] > compra_local['last_updated'] )
                print("Ejecutar el PULL")
        else:
            print(f"Compra cloud: {compra_local}")
            print("La compra no existe ejecutar el PULL")

    #create_replica_log(remoteDB,'PULL',sucursal['nombre'],'compra')