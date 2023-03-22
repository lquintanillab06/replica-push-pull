from datetime  import date,datetime
from src.database import get_local_pool_connection,get_remote_pool_connection
from src.services import obtener_diferencias,insert_or_update_entity,delete_entity,get_sucursal_local,crear_audit_operacion

def validar_inventario_hoy():
    hoy = date.today()
    validar_inventario(hoy)

def validar_inventario_fecha(fecha):
    validar_inventario(fecha)
    

def validar_inventario(fecha):
        
    sucursal_local = get_sucursal_local()

    if sucursal_local['nombre'] != 'OFICINAS':

        try:
            localDB = get_local_pool_connection()
        except Exception as e:
            print(e)

        try:
            remoteDB = get_remote_pool_connection()
        except Exception as e:
            print(e) 

        query_inventario = f"select * from inventario where date(fecha) = '{fecha}' "
        query_id = f"select id from inventario where date(fecha) = '{fecha}' "

        local_cnx =  localDB.get_conexion()
        local_cursor = local_cnx.cursor(dictionary=True, buffered=True)

        remote_cnx =  remoteDB.get_conexion()
        remote_cursor = remote_cnx.cursor(dictionary=True, buffered=True)

        local_cursor.execute(query_inventario)
        inventarios_local = local_cursor.fetchall()
        remote_cursor.execute(query_inventario)
        inventarios_remote = remote_cursor.fetchall()
        faltantes = obtener_diferencias(inventarios_local,inventarios_remote)
        print("inventarios_local:", len(inventarios_local))
        print("inventarios_remote:", len(inventarios_remote))
        print("faltantes", len(faltantes))

        local_cursor.execute(query_id)
        ids_local =local_cursor.fetchall()
        remote_cursor.execute(query_id)
        ids_remote =remote_cursor.fetchall()
        sobrantes = obtener_diferencias(ids_remote,ids_local)
        print("ids_local:", len(ids_local))
        print("ids_remote:", len(ids_remote))
        print("sobrantes", len(sobrantes))

        local_cnx.close()
        remote_cnx.close()

        for inventario in faltantes:
            print(inventario)
            insert_or_update_entity(remoteDB, 'inventario', inventario)
            audit = {
                'version': 0,
                'persisted_object_id': inventario['id'],
                'target': 'OFICINAS',
                'date_created': datetime.now(),
                'last_updated': datetime.now(),
                'name':'inventario',
                'event_name': 'INSERT',
                'table_name': 'inventario',
                'source': sucursal_local['nombre'],
            }
            crear_audit_operacion(remoteDB,audit)

        for inventario_id in sobrantes:
            print(inventario_id)
            delete_entity(remoteDB, 'inventario', inventario_id['id'])
            audit = {
                'version': 0,
                'persisted_object_id': inventario_id['id'],
                'target': 'OFICINAS',
                'date_created': datetime.now(),
                'last_updated': datetime.now(),
                'name':'inventario',
                'event_name': 'DELETE',
                'table_name': 'inventario',
                'source': sucursal_local['nombre'],
            }
            crear_audit_operacion(remoteDB,audit)
          

            
        

    




    


    




        