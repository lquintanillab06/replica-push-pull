import datetime
from src.services import (get_sucursal_local,insert_or_update_entity,create_replica_log,get_last_run_replica_log,get_entities,
                          get_replica_entity,get_replica_entity_by_field, insert_replica_entity)
from src.database import get_database_connections_pool

def replica_push_cobros(status = "normal"):
    print("*"*50)
    print("Ejecutando la replica push de cobros")
    print("*"*50)
    sucursal = get_sucursal_local()
    localDB, remoteDB = get_database_connections_pool()
    action = 'PUSH'
    fecha = datetime.datetime.today()
    print(f"Ejecutando el push de cobros !!!")
    if status == "normal":
        print("lastu run normal ")
        last_run = get_last_run_replica_log(remoteDB,fecha,'cobro',sucursal['nombre'],action) 
    else: 
        print(f"EL  last run no es normal  {fecha.date()} ")
        last_run = fecha.date()    
    print(f"Ultima corrida {last_run}")
    query = f"Select * from cobro where last_updated >= '{last_run}'"
    cobros = get_entities(localDB,query)
    for cobro in cobros:
        print("*"*60)
        print(cobro)
        insert_or_update_entity(remoteDB,'cobro',cobro)
        cobro_tipo,cobro_table = get_cobro_tipo(localDB,cobro)
        if cobro_tipo:
            print("-"*60)
            print(f"Cobro Tipo: {cobro_tipo}")
            insert_or_update_entity(remoteDB,cobro_table,cobro_tipo)
            if cobro_table == 'cobro_deposito' or cobro_table == 'cobro_transferencia':
                solicitud = get_replica_entity_by_field(localDB,'solicitud_de_deposito','cobro_id',cobro['id'])
                if solicitud:
                    print("+"*60)
                    print(f"Solicitud: {solicitud}")
                    insert_or_update_entity(remoteDB,'solicitud_de_deposito',solicitud)

    if status == "normal":
        create_replica_log(remoteDB,'PUSH',sucursal['nombre'],'cobro')




def replica_pull_cobros():
    sucursal = get_sucursal_local()
    if sucursal['nombre'] == 'OFICINAS':
        print("*"*50)
        print("Ejecutando la replica pull de cobros")
        print("*"*50)
        localDB, remoteDB = get_database_connections_pool()
        action = 'PULL'
        fecha = datetime.datetime.today()
        print(f"Ejecutando el pull de cobros !!!")
        last_run = get_last_run_replica_log(remoteDB,fecha,'cobro',sucursal['nombre'],action) 
        print(f"Ultima corrida {last_run}")
        query = f"Select * from cobro where last_updated >= '{last_run}'"
        cobros = get_entities(localDB,query)
        for cobro in cobros:
            print("*"*60)
            print(f"Cobro {cobro}")
            insert_or_update_entity(localDB,'cobro',cobro)
            cobro_tipo,cobro_table = get_cobro_tipo(remoteDB,cobro)
            if cobro_tipo:
                print("-"*60)
                print(f"Cobro Tipo: {cobro_tipo}")
                insert_or_update_entity(localDB,cobro_table,cobro_tipo)
                if cobro_table == 'cobro_deposito' or cobro_table == 'cobro_transferencia':
                    solicitud = get_replica_entity_by_field(remoteDB,'solicitud_de_deposito','cobro_id',cobro['id'])
                    if solicitud:
                        print("+"*60)
                        print(f"Solicitud: {solicitud}")
                        insert_or_update_entity(localDB,'solicitud_de_deposito',solicitud)
    else:
        print("No se puede realizar el  pull por no estar en oficinas !!!")

    #create_replica_log(remoteDB,'PULL',sucursal['nombre'],'cobro')


        
def get_cobro_tipo(connectionDB,cobro):
    
    print(f"Forma de pago : {cobro['forma_de_pago']}")

    if cobro['forma_de_pago'] == 'CHEQUE':
        cobro_tipo = get_replica_entity_by_field(connectionDB,'cobro_cheque','cobro_id',cobro['id'])
        return cobro_tipo,'cobro_cheque'

    if cobro['forma_de_pago'].startswith('DEPOSITO') :
        cobro_tipo = get_replica_entity_by_field(connectionDB,'cobro_deposito','cobro_id',cobro['id'])
        return cobro_tipo,'cobro_deposito'
    
    if cobro['forma_de_pago'].startswith('TRANSFERENCIA') :
        cobro_tipo = get_replica_entity_by_field(connectionDB,'cobro_transferencia','cobro_id',cobro['id'])
        return cobro_tipo,'cobro_transferencia'
    
    if cobro['forma_de_pago'].startswith('TARJETA') :
        cobro_tipo = get_replica_entity_by_field(connectionDB,'cobro_tarjeta','cobro_id',cobro['id'])
        return cobro_tipo,'cobro_tarjeta'
    
    return None, None