import datetime
from src.database import get_local_pool_connection, get_remote_pool_connection
from src.services import (get_entities,get_replica_entity, get_replica_entity_by_field,insert_or_update_entity,crear_audit,actualizar_audit,
                          get_last_run_replica_log,get_sucursal_local,create_replica_log)


def replica_push_cxc_con(status='normal'):
    replica_cxc_action('CON','PUSH', status)


def replica_push_cxc_cre(status='normal'):
    replica_cxc_action('CRE','PUSH',status)


def replica_push_cxc_cod(status='normal'):
    replica_cxc_action('COD','PUSH',status)


def replica_pull_cxc_con():
    replica_cxc_action('CON','PULL')


def replica_pull_cxc_cre():
    replica_cxc_action('CRE','PULL')


def replica_pull_cxc_cod():
    replica_cxc_action('COD','PULL')


def replica_cxc_action(tipo_cxc,action, status):
    try:
        localDB = get_local_pool_connection()
    except Exception as e:
        print(e)
    try:
        remoteDB = get_remote_pool_connection()
    except Exception as e:
        print(e)
    if action == 'PUSH':    
        replica_cxc(tipo_cxc,localDB,remoteDB, action,remoteDB,status)
    if action == 'PULL':
        replica_cxc(tipo_cxc,remoteDB,localDB, action,remoteDB,status)



def replica_cxc(tipo_cxc,origenDB,destinoDB,action, remoteDB, status):
    print(f"ORIGEN: {origenDB.database}")
    print(f"DESTINO: {destinoDB.database}")
    fecha = datetime.datetime.today()
    table = 'cuenta_por_cobrar'
    nametable = table + tipo_cxc
    sucursal = get_sucursal_local()
    if status == "normal":
        print("lastu run normal ")
        last_run = get_last_run_replica_log(destinoDB,fecha,nametable,sucursal['nombre'],action)  
        messageReplicated= 'Replicado Cloud'       
    else: 
        print(f"EL  last run no es normal  {fecha.date()} ")
        last_run = fecha.date()
        messageReplicated= 'Mensaje Replicado por una validacion global '       
    query_audit =   f"""
                    SELECT a.* 
                    FROM audit_log a join cfdi c on (c.id= a.persisted_object_id) join cuenta_por_cobrar u on (u.cfdi_id = c.id)
                    where table_name = 'cfdi'  and u.tipo = '{tipo_cxc}'
                    and a.date_created >='{last_run}' 
                    and a.replicated_cloud is null
                    """  
    print(query_audit)
    audits = get_entities(origenDB,query_audit)
    print(audits) 
    print(len(audits))
    if (len(audits) == 0):
        print("vacios")
        create_replica_log(remoteDB,action,sucursal['nombre'],nametable)
    else:
        for audit in audits:
            cfdi = get_replica_entity(origenDB, 'cfdi', audit['persisted_object_id'])
            if cfdi:
                insert_or_update_entity(destinoDB, 'cfdi', cfdi)
                cxc = get_replica_entity_by_field(origenDB,'cuenta_por_cobrar','cfdi_id',cfdi['id'])
                if cxc :
                    insert_or_update_entity(destinoDB, 'cuenta_por_cobrar', cxc)
                    venta = get_replica_entity_by_field(origenDB,'venta','cuenta_por_cobrar_id',cxc['id'])
                    if venta :
                        insert_or_update_entity(destinoDB, 'venta', venta)
                        query_venta_det = f"select * from venta_det where venta_id = '{venta.get('id')}' "
                        ventas_det = get_entities(origenDB,query_venta_det)
                        condicion = get_replica_entity_by_field(origenDB,'condicion_de_envio','venta_id',venta['id'])
                        if condicion:
                            insert_or_update_entity(destinoDB, 'condicion_de_envio', condicion)
                        for venta_det in ventas_det:                            
                            inventario = get_replica_entity(origenDB, 'inventario', venta_det['inventario_id'])
                            if inventario :
                                insert_or_update_entity(destinoDB, 'inventario', inventario)
                            instruccion = get_replica_entity_by_field(origenDB,'instruccion_corte','venta_det_id',venta_det['id'])
                            if instruccion:
                                insert_or_update_entity(destinoDB, 'instruccion_de_corte', instruccion)

                            insert_or_update_entity(destinoDB, 'venta_det', venta_det)
            if action == 'PUSH':
                crear_audit(destinoDB,audit['target'], audit, sucursal['nombre'])
            actualizar_audit(origenDB,'audit_log',audit['id'],messageReplicated)

        if status == "normal":
            create_replica_log(remoteDB,action,sucursal['nombre'],nametable)

        


   
