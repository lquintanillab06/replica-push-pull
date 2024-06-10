from datetime import datetime
from src.database import get_database_connections_pool
from src.services import (get_entities,get_replica_entity, get_replica_entity_by_field,insert_or_update_entity,crear_audit,actualizar_audit,
                          get_audits,get_sucursal_local, get_last_run_replica_log, create_replica_log)



def replica_pull_cobranza():
    print ("Ejecutando el pull de cobranza")
    sucursal = get_sucursal_local()
    if sucursal['nombre'] == 'OFICINAS':
        localDB, remoteDB = get_database_connections_pool()   
        action = 'PULL'
        replica_cobranza(remoteDB,localDB,action)
    else:
        print("No se puede ejecutar el pull porque no esta en oficinas")

def replica_push_cobranza():
    sucursal = get_sucursal_local()
    if sucursal['nombre'] != 'OFICINAS':
        localDB, remoteDB = get_database_connections_pool()   
        action = 'PUSH'
        replica_cobranza(localDB,remoteDB,action,remoteDB)
    else:
        print("No se puede hacer push por estar en oficinas")


def replica_cobranza(origenDB,destinoDB, action,remoteDB):
    fecha = datetime.today()
    table = 'aplicacion_de_cobro'
    sucursal = get_sucursal_local()
    last_run = get_last_run_replica_log(remoteDB,fecha,table,sucursal['nombre'],action) 
    print("Obteniendo audits")
    audits = get_audits(origenDB, destinoDB,'audit_log' ,action,'aplicacion_de_cobro',last_run)
    print("Se obutvieron los audits")
    print()
    for audit in audits:
        print(f"Audit: {audit['persisted_object_id']}")
        if audit['event_name'] == 'INSERT' or audit['event_name'] == 'UPDATE':
            aplicacion = get_replica_entity(origenDB,'aplicacion_de_cobro',audit['persisted_object_id'])
            if aplicacion:
                print(f"Aplicacion: {aplicacion['id']}")
                cobro = get_replica_entity(origenDB,'cobro',aplicacion['cobro_id'])
                if cobro:
                    print(f"Cobro: {cobro['id']}")
                    insert_or_update_entity(destinoDB,'cobro',cobro)
                    cobro_tipo,cobro_table = get_cobro_tipo(origenDB,cobro)
                    if cobro_tipo:
                        print(f"Cobro Tipo: {cobro_tipo}")
                        insert_or_update_entity(destinoDB,cobro_table,cobro_tipo)
                cxc = get_replica_entity(origenDB,'cuenta_por_cobrar',aplicacion['cuenta_por_cobrar_id'])
                if cxc:
                    print(f"Cxc: {cxc['id']}")
                    cfdi = get_replica_entity(origenDB,'cfdi',cxc['cfdi_id'])
                    if cfdi:
                        print(f"Cfdi: {cfdi['id']}")
                        insert_or_update_entity(destinoDB,'cfdi',cfdi)
                    insert_or_update_entity(destinoDB,'cuenta_por_cobrar',cxc)
                    venta = get_replica_entity_by_field(origenDB, 'venta', 'cuenta_por_cobrar_id', cxc['id'])
                    if venta:
                        print(f"Venta: {venta['id']}")
                        insert_or_update_entity(destinoDB,'venta',venta)
                        condicion = get_replica_entity_by_field(origenDB,'condicion_de_envio','venta_id',venta['id'])
                        if condicion:
                            print(f"Condicion: {condicion['id']}")
                            insert_or_update_entity(destinoDB, 'condicion_de_envio', condicion)
                        query_venta_det = f"select * from venta_det where venta_id = '{venta.get('id')}' "
                        ventas_det = get_entities(origenDB,query_venta_det)
                        for det in ventas_det:
                            print(f"VentaDet: {det['id']}")
                            inventario = get_replica_entity(origenDB,'inventario',det['inventario_id'])
                            if inventario:
                                print(f"VentaDet: {inventario['id']}")
                                insert_or_update_entity(destinoDB, 'inventario', inventario)
                            insert_or_update_entity(destinoDB, 'venta_det', det)
                            instruccion = get_replica_entity_by_field(origenDB,'instruccion_corte','venta_det_id',det['id'])
                            if instruccion:
                                print(f"Instruccion: {instruccion['id']}")
                                insert_or_update_entity(destinoDB, 'instruccion_corte', instruccion)

            

            else:
                print("No hay aplicacion")   
                print(audit)
            if action == 'PUSH':
                target = 'OFICINAS' if audit['target'] == 'CENTRAL' else audit['target']
                crear_audit(destinoDB,target, audit,sucursal['nombre'])

            print(" APLICACION INSERTANDO ")
            print(f"aplicacion a insertar   {aplicacion}" )
            insert_or_update_entity(destinoDB,'aplicacion_de_cobro',aplicacion)
            actualizar_audit(origenDB,'audit_log',audit['id'],'Replicado Cloud') 
           


            print("+"*50)
        else:               
            print("El event name del audit es DELETE")

    create_replica_log(remoteDB,action,sucursal['nombre'],table)


def get_cobro_tipo(connectionDB,cobro):
    
    print(f"Forma de pago : {cobro['forma_de_pago']}")

    if cobro['forma_de_pago'] == 'CHEQUE':
        print("Cobro con cheque")
        cobro_tipo = get_replica_entity_by_field(connectionDB,'cobro_cheque','cobro_id',cobro['id'])
        print("+"*60)
        print(cobro_tipo)
        print("+"*60)
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