import datetime
from src.services import (get_sucursal_local,insert_or_update_entity,create_replica_log,get_last_run_replica_log,get_entities,
                          get_replica_entity,get_replica_entity_by_field, insert_replica_entity)
from src.database import get_database_connections_pool



def replica_push_devolucion_compra():
    print("*"*50)
    print("Ejecutando la replica push de devolucion de compras")
    print("*"*50)

    sucursal = get_sucursal_local()
    localDB, remoteDB = get_database_connections_pool()
    action = 'PUSH'
    fecha = datetime.datetime.today()
    print(f"Ejecutando el push de devolucion de compras !!!")
    last_run = get_last_run_replica_log(remoteDB,fecha,'devolucion_de_compra',sucursal['nombre'],action) 
    print(f"Ultima corrida {last_run}")

    query = f"Select * from devolucion_de_compra where last_updated >= '{last_run}'"
    devoluciones = get_entities(localDB,query)
    print(devoluciones)
    for devolucion in devoluciones:
        print(devolucion['last_updated'])
        devolucion_cloud = get_replica_entity(remoteDB,'devolucion_de_compra',devolucion['id'])
        
        if devolucion_cloud:
            print("La devolucion existe actualizar")
            print(devolucion_cloud)
            if devolucion['last_updated'] > devolucion_cloud['last_updated']:
                print("La devolucion de compra local es mas reciente actualizar !!!")
                insert_or_update_entity(remoteDB, 'devolucion_de_compra', devolucion)
                query_devolucion_det = f"select * from devolucion_de_compra_det where devolucion_de_compra_id = '{devolucion.get('id')}' "
                devoluciones_det = get_entities(localDB,query_devolucion_det)
                for devolucion_det in devoluciones_det:
                    print(f"Com Det {devolucion_det['id']}")
                    devolucion_det['compra_det_id'] = None
                    inventario = get_replica_entity(localDB,'inventario',devolucion_det['inventario_id'])
                    if inventario:
                        print(f"Inventario : {inventario['id']}")
                        insert_or_update_entity(remoteDB, 'inventario', inventario)
                    insert_or_update_entity(remoteDB, 'devolucion_de_compra_det', devolucion_det)


        else:
            print("La devolucion no existe insertar")
            insert_or_update_entity(remoteDB, 'devolucion_de_compra', devolucion)
            query_devolucion_det = f"select * from devolucion_de_compra_det where devolucion_de_compra_id = '{devolucion.get('id')}' "
            devoluciones_det = get_entities(localDB,query_devolucion_det)
            for devolucion_det in devoluciones_det:
                print(f"Com Det {devolucion_det['id']}")
                devolucion_det['compra_det_id'] = None
                inventario = get_replica_entity(localDB,'inventario',devolucion_det['inventario_id'])
                if inventario:
                    print(f"Inventario : {inventario['id']}")
                    insert_or_update_entity(remoteDB, 'inventario', inventario)
                insert_or_update_entity(remoteDB, 'devolucion_de_compra_det', devolucion_det)
        print("-"*50)
            #create_replica_log(remoteDB,'PUSH',sucursal['nombre'],'devolucion_de_compra')
            
def replica_pull_devolucion_compra():
    sucursal = get_sucursal_local()
    localDB, remoteDB = get_database_connections_pool()
    action = 'PULL'
    fecha = datetime.datetime.today()
    print(f"Ejecutando el pull de devolucion de compras !!!")
    last_run = get_last_run_replica_log(remoteDB,fecha,'devolucion_de_compra',sucursal['nombre'],action) 
    print(f"Ultima corrida {last_run}")

    if sucursal['nombre'] == 'OFICINAS':
        print("Inicializando el PULL")
        query = f"Select * from devolucion_de_compra where last_updated >= '{last_run}'"
        devoluciones = get_entities(remoteDB,query)
        #print(devoluciones)
        for devolucion in devoluciones:
            print(devolucion['id'])
            print(devolucion['last_updated'])
            devolucion_local = get_replica_entity(remoteDB,'devolucion_de_compra',devolucion['id'])
            if devolucion_local:
                print("La devoucion existe ")
                if devolucion['last_updated'] > devolucion_local['last_updated']:
                    print("La devolucion remota es mas reciente que la local actualizar !!!")
                    insert_or_update_entity(localDB, 'devolucion_de_compra', devolucion)
                    query_devolucion_det = f"select * from devolucion_de_compra_det where devolucion_de_compra_id = '{devolucion.get('id')}' "
                    devoluciones_det = get_entities(remoteDB,query_devolucion_det)
                    for devolucion_det in devoluciones_det:
                        print(f"Com Det {devolucion_det['id']}")
                        devolucion_det['compra_det_id'] = None
                        inventario = get_replica_entity(remoteDB,'inventario',devolucion_det['inventario_id'])
                        if inventario:
                            print(f"Inventario : {inventario['id']}")
                            insert_or_update_entity(localDB, 'inventario', inventario)
                        insert_or_update_entity(localDB, 'devolucion_de_compra_det', devolucion_det)
            else:
                print("La devolucion no existe insertar")
                insert_or_update_entity(localDB, 'devolucion_de_compra', devolucion)
                query_devolucion_det = f"select * from devolucion_de_compra_det where devolucion_de_compra_id = '{devolucion.get('id')}' "
                devoluciones_det = get_entities(remoteDB,query_devolucion_det)
                for devolucion_det in devoluciones_det:
                    print(f"Com Det {devolucion_det['id']}")
                    devolucion_det['compra_det_id'] = None
                    inventario = get_replica_entity(remoteDB,'inventario',devolucion_det['inventario_id'])
                    if inventario:
                        print(f"Inventario : {inventario['id']}")
                        insert_or_update_entity(localDB, 'inventario', inventario)
                    insert_or_update_entity(localDB, 'devolucion_de_compra_det', devolucion_det)
            print("-"*50)    
    else:
        print("No se puede realizar el pull por no estar en oficinas !!!")

    create_replica_log(remoteDB,'PULL',sucursal['nombre'],'devolucion_de_compra')

