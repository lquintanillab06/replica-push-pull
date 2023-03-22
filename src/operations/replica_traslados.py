from src.services import get_audits,get_sucursal_local,get_replica_entity, get_entities,insert_or_update_entity, crear_audit,actualizar_audit
from src.database import get_database_connections_pool


def replica_push_traslados():
    localDB, remoteDB = get_database_connections_pool()   
    localDB, remoteDB = get_database_connections_pool()
    sucursal = get_sucursal_local()
    action = 'PUSH'
    audits = get_audits(localDB, remoteDB,'audit_log' ,action,'traslado')
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
                print(f"TrasladoDet: {det['id']}")
                insert_or_update_entity(remoteDB, 'traslado_det', det)
            sucursal_traslado = get_replica_entity(localDB,'sucursal',traslado['sucursal_id'])
            print(f"Sucursal: {sucursal_traslado['nombre']}")
            print("Procedemos a crear el auditlog en la nube")
            crear_audit(remoteDB,'OFICINAS', audit)
            if sucursal['id'] != sucursal_traslado['id']:
                crear_audit(remoteDB,sucursal_traslado['nombre'], audit)
            actualizar_audit(localDB,'audit_log',audit['id'],'Replicado')
        elif not traslado  and audit['event_name'] == 'DELETE':
            print("Ejecutar delete")
 

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