from datetime import datetime
from src.database import get_database_connections_pool
from src.services import (get_entities,get_replica_entity, get_replica_entity_by_field,insert_or_update_entity,crear_audit,actualizar_audit,
                          get_audits,get_sucursal_local)




def demo():
    localDB, remoteDB = get_database_connections_pool() 
    query_suc = f"select i.documento,i.tipo,s.nombre,i.cantidad from inventario i join sucursal  s on (s.id = i.sucursal_id) where fecha >= '2023/03/23'"
    sucursales = get_entities(localDB,query_suc)
    print(sucursales)
    for  sucursal in sucursales:
        print(sucursal) 
        print(sucursal['cantidad'] * 10)
        sucursal['cantidad'] = sucursal['cantidad'] * 10
        print(sucursal)
        print("*"*50)
        #insert_or_update_entity(remoteDB,'sucursal',sucursal)
       