import datetime
from src.services import (get_sucursal_local,insert_or_update_entity,create_replica_log,get_last_run_replica_log,get_entities,
                          get_replica_entity,get_replica_entity_by_field, insert_replica_entity)
from src.database import get_database_connections_pool

def replica_push_cancelacion_cfdi():
    print("*"*50)
    print("Ejecutando la replica push de cancelacion de  cfdi")
    print("*"*50)
    sucursal = get_sucursal_local()
    if sucursal['nombre'] != 'OFICINAS':
        localDB, remoteDB = get_database_connections_pool()
        action = 'PUSH'
        fecha = datetime.datetime.today()
        print(f"Ejecutando el push de cancelacion de cfdi !!!")
        last_run = get_last_run_replica_log(remoteDB,fecha,'cancelacion_cfdi',sucursal['nombre'],action) 
        print(f"Ultima corrida {last_run}")
        query = f"Select * from cfdi where last_updated >= '{last_run}' and status = 'CANCELACION_PENDIENTE'"
        cfdis = get_entities(localDB,query)
        for cfdi in cfdis:
            print("*"*60)
            print(cfdi)
            cfdi_cloud = get_replica_entity(remoteDB,'cfdi',cfdi['id'])
            if cfdi_cloud:
                print("Validar la informacion en cloud para desvincular y borrar")
                cxc_cloud = get_replica_entity_by_field(remoteDB,'cuenta_por_cobrar','cfdi_id',cfdi['id'])
                if cxc_cloud:
                    print("Buscar informacion de la venta para desvincular")
                    venta_cloud = get_replica_entity(remoteDB,'venta',cxc_cloud['id'])
                    if venta_cloud:
                        query_venta_det = f"Select * from venta_det where venta_id = '{venta_cloud['id']}'"
                        ventas_det = get_entities(remoteDB,query_venta_det)
                        for venta_det in ventas_det:
                            print("Usar la venta det para buscar los inventarios y borrarlos")
                            inventario = get_replica_entity(remoteDB,'inventario',ventas_det['inventario_id'])
                            if inventario:
                                print("Borrar el inventario que pertenece a una venta cancelada")
                        print("Desvicular la venta")

                    print("Buscar la aplicacion de cobro apartir de aqui buscar el cobro para actualizar o borrar")
                    aplicacion_cloud = get_replica_entity_by_field(remoteDB,'aplicacion_de_cobro','cuenta_por_cobrar_id',cxc_cloud['id'])   
                    if aplicacion_cloud:
                        print("Buscar cobro") 
                        cobro = get_replica_entity(remoteDB,'cobro',aplicacion_cloud['cobro_id']) 
                        if cobro['forma_de_pago'] == 'EFECTIVO' or cobro['forma_de_pago'] =='CHEQUE' or cobro['forma_de_pago'].startswith('TARJETA'):
                            print("Borrar cobro y su complemento")
                            cobro_tipo,cobro_table = get_cobro_tipo(remoteDB,cobro)
                            if cobro_tipo:
                                print("Borrar el cobro tipo")
                                print("Borrar el cobro")
                        print("Borrar aplicacion de cobro")
                    print("Actualizar la cuenta por cobrar")
            print("Actualizar cfdi")
            insert_or_update_entity(remoteDB,'cfdi',cfdi)
        create_replica_log(remoteDB,'PUSH',sucursal['nombre'],'cancelacion_cfdi')



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