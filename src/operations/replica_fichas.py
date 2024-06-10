import datetime
from src.services import (get_sucursal_local,insert_or_update_entity,create_replica_log,get_last_run_replica_log,get_entities,
                          get_replica_entity, insert_replica_entity)
from src.database import get_database_connections_pool



def replica_push_fichas():
    sucursal = get_sucursal_local()

    if sucursal['nombre'] != 'OFICINAS':

        localDB, remoteDB = get_database_connections_pool()
        action = 'PUSH'
        fecha = datetime.datetime.today()
        print(f"Ejecutando el push del movimiento !!! fichas")
        last_run = get_last_run_replica_log(remoteDB,fecha,'ficha',sucursal['nombre'],action) 
        print(f"Ultima corrida {last_run}")
        query = f"select * from ficha where (date_created >= '{last_run}' or last_updated >= '{last_run}' ) and sucursal_id = '{sucursal['id']}'"
        fichas = get_entities(localDB,query)

        for ficha in fichas:

            print(ficha['id'])
            insert_or_update_entity(remoteDB,'ficha',ficha)
            if ficha['tipo_de_ficha'] == 'EFECTIVO':
    
                print('ficha de efectivo solo se importara la ficha')

            else:

                print('ficha de cheques importar cobro cheque y cobro')
                query_cheques = f"select * from cobro_cheque where ficha_id = '{ficha['id']}'"
                cheques = get_entities(localDB,query_cheques)
                for cheque in cheques:

                    print(f"Cobro cheque: {cheque['id']}") 
                    cobro = get_replica_entity(localDB, 'cobro', cheque['cobro_id'])
                    print(f"Cobro: {cobro['id']}")
                    if cobro:
                        insert_or_update_entity(remoteDB,'cobro',cobro)
                    insert_or_update_entity(remoteDB,'cobro_cheque',cheque)

        if fichas:

            create_replica_log(remoteDB,'PUSH',sucursal['nombre'],'ficha')

    create_replica_log(remoteDB,'PUSH',sucursal['nombre'],'ficha')


def replica_pull_fichas():
    sucursal = get_sucursal_local()

    if sucursal['nombre'] == 'OFICINAS':

        localDB, remoteDB = get_database_connections_pool()
        action = 'PULL'
        fecha = datetime.datetime.today()
        print(f"Ejecutando el pull del movimiento !!! fichas")
        last_run = get_last_run_replica_log(remoteDB,fecha,'ficha',sucursal['nombre'],action) 
        print(f"Ultima corrida {last_run}")
        query = f"select * from ficha where (date_created >= '{last_run}' or last_updated >= '{last_run}' )"
        fichas = get_entities(remoteDB,query)

        for ficha in fichas:

            print(ficha['id'])
            insert_or_update_entity(localDB,'ficha',ficha)
            if ficha['tipo_de_ficha'] == 'EFECTIVO':

                print('ficha de efectivo solo se importara la ficha')

            else:

                print('ficha de cheques importar cobro cheque y cobro')
                query_cheques = f"select * from cobro_cheque where ficha_id = '{ficha['id']}'"
                cheques = get_entities(remoteDB,query_cheques)
                for cheque in cheques:
                    
                    print(f"Cobro cheque: {cheque['id']}") 
                    cobro = get_replica_entity(remoteDB, 'cobro', cheque['cobro_id'])
                    print(f"Cobro: {cobro['id']}")
                    if cobro:
                        insert_or_update_entity(localDB,'cobro',cobro)
                    insert_or_update_entity(localDB,'cobro_cheque',cheque)

        if fichas:

            create_replica_log(remoteDB,'PULL',sucursal['nombre'],'ficha')
    else:
        print("No se puede ejecutar la accion pull porque no esta en oficinas !!!")
