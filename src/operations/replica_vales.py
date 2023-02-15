from src.services import get_audits, get_maestro_detalle, pull_entity, push_entity,actualizar_audit,crear_audit,get_replica_entity
from src.database import get_local_pool_connection,get_remote_pool_connection



def replica_pull_vales():
    replica_vales('PULL')

def replica_push_vales():
    replica_vales('PUSH')

def replica_vales(action):
    try:
        localDB = get_local_pool_connection()
    except Exception as e:
        print(e)

    try:
        remoteDB = get_remote_pool_connection()
    except Exception as e:
        print(e)
        
    audits = get_audits(localDB, remoteDB,'audit_log' ,action,'solicitud_de_traslado')
    for audit in audits:
        sol, partidas = get_sol(localDB,remoteDB,action,audit['persisted_object_id'])
        if action == 'PULL':
            replicado, error = pull_entity(localDB,remoteDB,'solicitud_de_traslado',sol['id'],audit['event_name'])
        if action == 'PUSH':
            replicado, error = push_entity(localDB,remoteDB,'solicitud_de_traslado',sol['id'],audit['event_name'])   
        for partida in partidas:
            if action == 'PULL':
                replicado_part, error_part = pull_entity(localDB,remoteDB,'solicitud_de_traslado_det',partida['id'],audit['event_name'])
            if action == 'PUSH':
                replicado_part, error_part = push_entity(localDB,remoteDB,'solicitud_de_traslado_det',partida['id'],audit['event_name'])
        if action == 'PULL':
            if replicado:
                actualizar_audit(remoteDB,'audit_log',audit['id'], 'Replicado')
            if error:
                 actualizar_audit(remoteDB,'audit_log',audit['id'], 'Error')
        if action == 'PUSH':
            if replicado:
                atiende = get_replica_entity(localDB,'sucursal',sol['sucursal_atiende_id'])
                solicita = get_replica_entity(localDB,'sucursal',sol['sucursal_solicita_id'])
                crear_audit(remoteDB,'OFICINAS', audit)
                crear_audit(remoteDB,atiende['nombre'], audit)
                crear_audit(remoteDB,solicita['nombre'], audit)
                actualizar_audit(localDB,'audit_log',audit['id'])
            if error:
                actualizar_audit(localDB,'audit_log',audit['id'])

def get_sol(localDB,remoteDB,action,id):
    query_sol = f"select * from solicitud_de_traslado where id = '{id}' " 
    query_partidas = f"select * from solicitud_de_traslado_det where solicitud_de_traslado_id = '{id}' " 
    if action == 'PULL':
        sol, partidas =  get_maestro_detalle(remoteDB,query_sol,query_partidas)
        return sol, partidas 
    if action == 'PUSH':
        sol, partidas =  get_maestro_detalle(localDB,query_sol,query_partidas)
        return sol, partidas 
