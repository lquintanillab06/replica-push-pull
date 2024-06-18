from src.services import (get_audits,get_sucursal_local,get_replica_entity, get_entities,insert_or_update_entity, crear_audit,actualizar_audit,
                        get_last_run_replica_log, create_replica_log)
from src.database import get_database_connections_pool
import datetime


def replica_push_traslados(status = 'normal'):
    localDB, remoteDB = get_database_connections_pool()   
    localDB, remoteDB = get_database_connections_pool()
    sucursal = get_sucursal_local()
    action = 'PUSH'
    fecha = datetime.datetime.today()
    table = 'traslado'
    sucursal = get_sucursal_local()
    last_run = get_last_run_replica_log(remoteDB,fecha,table,sucursal['nombre'],action) 
    if status == "normal":
        print("lastu run normal ")
        last_run = get_last_run_replica_log(remoteDB,fecha,table,sucursal['nombre'],action)    
        messageReplicated = "Replicated cloud"               
    else: 
        print(f"EL  last run no es normal  {fecha.date()} ")
        last_run = fecha.date()    
        messageReplicated = "Replicated cloud"  
    audits = get_audits(localDB, remoteDB,'audit_log' ,action,'traslado', last_run)
    print(len(audits))
    for audit in audits:
        #print(audit)
        traslado = get_replica_entity(localDB,'traslado',audit['persisted_object_id'])
        if traslado:
            print(f"Traslado: {traslado['tipo']} {traslado['id']}")
            cfdi = get_replica_entity(localDB,'cfdi',traslado['cfdi_id'])
            if cfdi:
                print(f"Cfdi : {cfdi['id']}")
                insert_or_update_entity(remoteDB, 'cfdi', cfdi)
            insert_or_update_entity(remoteDB, 'traslado', traslado)
            query_traslado_det = f"select * from traslado_det where traslado_id = '{traslado.get('id')}' "
            traslados_det = get_entities(localDB,query_traslado_det)
            for det in traslados_det:
                if det["inventario_id"]:
                    inventario = get_replica_entity(localDB,'inventario',det['inventario_id'])
                    if inventario:
                        insert_or_update_entity(remoteDB, 'inventario', inventario)
                print(f"TrasladoDet: {det['id']}")
                insert_or_update_entity(remoteDB, 'traslado_det', det)
            sucursal_traslado = get_replica_entity(localDB,'sucursal',traslado['sucursal_id'])
            print(f"Sucursal: {sucursal_traslado['nombre']}")
            print("Procedemos a crear el auditlog en la nube")
            crear_audit(remoteDB,'OFICINAS', audit,sucursal['nombre'])
            if sucursal['id'] != sucursal_traslado['id']:
                crear_audit(remoteDB,sucursal_traslado['nombre'], audit,sucursal['nombre'])
            actualizar_audit(localDB,'audit_log',audit['id'],messageReplicated)
        elif not traslado  and audit['event_name'] == 'DELETE':
            print("Ejecutar delete")
    
    if status == "normal":
        create_replica_log(remoteDB,action,sucursal['nombre'],table)

    


def replica_pull_traslados():
    localDB, remoteDB = get_database_connections_pool()   
    localDB, remoteDB = get_database_connections_pool()
    sucursal = get_sucursal_local()
    action = 'PULL'
    audits = get_audits(localDB, remoteDB,'audit_log' ,action,'traslado')
    print(len(audits))
    for audit in audits:
        print(audit)
        traslado = get_replica_entity(remoteDB,'traslado',audit['persisted_object_id'])
        if traslado:
            print(f"Traslado: {traslado['tipo']} {traslado['id']}")
            cfdi = get_replica_entity(remoteDB,'cfdi',traslado['cfdi_id'])
            if cfdi:
                print(f"Cfdi : {cfdi['id']}")
                insert_or_update_entity(localDB, 'cfdi', cfdi)
            insert_or_update_entity(localDB, 'traslado', traslado)
            query_traslado_det = f"select * from traslado_det where traslado_id = '{traslado.get('id')}' "
            traslados_det = get_entities(remoteDB,query_traslado_det)
            for det in traslados_det:
                print(f"TrasladoDet: {det['id']}")
                insert_or_update_entity(localDB, 'traslado_det', det)
            actualizar_audit(remoteDB,'audit_log',audit['id'],'Replicado')
        elif not traslado  and audit['event_name'] == 'DELETE':
            print("Ejecutar delete")
        #actualizar_audit(remoteDB,'audit_log',audit['id'],'Replicado')