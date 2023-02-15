import datetime
from src.services import (get_maestro_detalle,get_replica_entity,get_entities,get_last_run_replica_log,get_sucursal_local,insert_or_update_entity,create_replica_log,
                            get_sucursales_replica)
from src.database import get_local_pool_connection,get_remote_pool_connection


from multiprocessing import Process,Pool


def replica_mp_movimiento_almacen():
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
    action = 'PUSH'
    sucursales = get_sucursales_replica(localDB)
    
    ''' p1 = Process(target=query_sucursal,args=(sucursales[0],))
    p1.start() '''
    '''  p2 = Process(target=query_sucursal,args=(sucursales[1],))
    p2.start()
    p3 = Process(target=query_sucursal,args=(sucursales[2],))
    p3.start()
    p4 = Process(target=query_sucursal,args=(sucursales[3],))
    p4.start() '''
   


    for sucursal in sucursales:
        print("*"*100)
        print(sucursal['server'])
        #query_sucursal(sucursal) 
        p = Process(target=query_sucursal,args=(sucursal,))
        p.start() 


def query_sucursal(sucursal_replica):
    localDB = get_local_pool_connection()
    print("Running en Sucursal....")
    print(sucursal_replica['server'])
    cnx = localDB.get_conexion()
    cursor = cnx.cursor(dictionary=True, buffered=True)
    sql_sucursal = f"select * from sucursal where nombre = '{sucursal_replica['server']}'  "
    cursor.execute(sql_sucursal)
    sucursal = cursor.fetchone()
    print(sucursal)
    query = f"select * from inventario where sucursal_id = '{sucursal['id']}' and fecha >='2022/12/15' limit 10 "
    cursor.execute(query)
    movs = cursor.fetchall()
    cnx.close()

    for mov in movs:
        print(f" Trabajando en {sucursal['nombre']}")
        print(f"Mov: {mov['tipo']} -- {mov['id']}") 
