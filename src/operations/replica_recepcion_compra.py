import datetime
from src.services import (get_sucursal_local,insert_or_update_entity,create_replica_log,get_last_run_replica_log,get_entities,
                          get_replica_entity,get_replica_entity_by_field, insert_replica_entity)
from src.database import get_database_connections_pool



def replica_push_recepcion_compra():
    print("*"*50)
    print("Ejecutando la replica push de recepcion de compras")
    print("*"*50)
    print("*"*50)
    print("Ejecutando la replica push de compras")
    print("*"*50)

    sucursal = get_sucursal_local()
    localDB, remoteDB = get_database_connections_pool()
    action = 'PUSH'
    fecha = datetime.datetime.today()
    print(f"Ejecutando el push de recepcion de compras !!!")
    last_run = get_last_run_replica_log(remoteDB,fecha,'recepcion_de_compra',sucursal['nombre'],action) 
    print(f"Ultima corrida {last_run}")

    query = f"Select * from recepcion_de_compra where last_updated >= '{last_run}'"
    coms = get_entities(localDB,query)
    #print(coms)
 
    for com in coms:
        print("-"*50)
        print(com['last_updated'])
        com_cloud = get_replica_entity(remoteDB,'recepcion_de_compra',com['id'])
        print("Com cloud ...")
        if com_cloud:
            print("El COM existe!!!")
            if com['last_updated'] > com_cloud['last_updated']:
                insert_or_update_entity(remoteDB, 'recepcion_de_compra', com)
                print("El com origen es mas reciente actualizar !!!")
                query_com_det = f"select * from recepcion_de_compra_det where recepcion_id = '{com.get('id')}' "
                coms_det = get_entities(localDB,query_com_det)
                for com_det in coms_det:
                    print(f"Com Det {com_det['id']}")
                    insert_or_update_entity(remoteDB, 'recepcion_de_compra_det', com_det)
                    inventario = get_replica_entity(localDB,'inventario',com_det['inventariox'])
                    if inventario:
                        print(f"Inventario : {inventario['id']}")
                        insert_or_update_entity(remoteDB, 'inventario', inventario)
            
        
        else :
            print("El COM no existe se debe insertar !!!")
            insert_or_update_entity(remoteDB, 'recepcion_de_compra', com)
            query_com_det = f"select * from recepcion_de_compra_det where recepcion_id = '{com.get('id')}' "
            coms_det = get_entities(localDB,query_com_det)
            for com_det in coms_det:
                print(f"Com Det {com_det['id']}")
                insert_or_update_entity(remoteDB, 'recepcion_de_compra_det', com_det)
                inventario = get_replica_entity(localDB,'inventario',com_det['inventariox'])
                if inventario:
                    print(f"Inventario : {inventario['id']}")
                    insert_or_update_entity(remoteDB, 'inventario', inventario)

    #create_replica_log(remoteDB,'PUSH',sucursal['nombre'],'recepcion_de_compra')


def replica_pull_recepcion_compra():
    sucursal = get_sucursal_local()
    localDB, remoteDB = get_database_connections_pool()
    action = 'PULL'
    fecha = datetime.datetime.today()
    print(f"Ejecutando el pull de recepcion de compras !!!")
    last_run = get_last_run_replica_log(remoteDB,fecha,'recepcion_de_compra',sucursal['nombre'],action) 
    print(f"Ultima corrida {last_run}")

    if sucursal['nombre'] == 'OFICINAS':
        print("Inicializando el PULL")
        query = f"Select * from recepcion_de_compra where last_updated >= '{last_run}'"
        coms = get_entities(remoteDB,query)

        for com in coms:
            print(com['id'])
            com_local = get_replica_entity(localDB,'recepcion_de_compra',com['id'])
            
            if com_local:
                print("Com local ...")
                print(com_local['id'])
                print("El com existe !!!")
                if com['last_updated'] > com_local['last_updated']:
                    insert_or_update_entity(localDB, 'recepcion_de_compra', com)
                    print ("El com original es mas reciente que el local actualizar")
                    query_com_det = f"select * from recepcion_de_compra_det where recepcion_id = '{com.get('id')}' "
                    coms_det = get_entities(remoteDB,query_com_det)
                    for com_det in coms_det:
                        print(f"Com Det {com_det['id']}")
                        insert_or_update_entity(localDB, 'recepcion_de_compra_det', com_det)
                        inventario = get_replica_entity(remoteDB,'inventario',com_det['inventariox'])
                        if inventario:
                            print(f"Inventario : {inventario['id']}")
                            insert_or_update_entity(localDB, 'inventario', inventario)
                   
            else:
                print("El com no existe insertar !!!")
                insert_or_update_entity(localDB, 'recepcion_de_compra', com)
                print ("El com original es mas reciente que el local actualizar")
                query_com_det = f"select * from recepcion_de_compra_det where recepcion_id = '{com.get('id')}' "
                coms_det = get_entities(remoteDB,query_com_det)
                for com_det in coms_det:
                    print(f"Com Det {com_det['id']}")
                    insert_or_update_entity(localDB, 'recepcion_de_compra_det', com_det)
                    inventario = get_replica_entity(remoteDB,'inventario',com_det['inventariox'])
                    if inventario:
                        print(f"Inventario : {inventario['id']}")
                        insert_or_update_entity(localDB, 'inventario', inventario)
                   
    else:


        print("No se puede realizar el pull por no estar en oficinas !!!")

    #create_replica_log(remoteDB,'PULL',sucursal['nombre'],'recepcion_de_compra')