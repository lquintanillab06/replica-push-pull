from src.services import (get_audits, get_maestro_detalle, pull_entity, push_entity,actualizar_audit,crear_audit,get_replica_entity,
                          get_sucursal_local,get_last_run_replica_log, create_replica_log)
from src.database import get_local_pool_connection,get_remote_pool_connection
import datetime

try:
    remoteDB = get_remote_pool_connection()
except Exception as e:
    print(e)

def replica_pull_vales(status='normal'):
    print("Replica pull de Vales ...",datetime.datetime.now())
    replica_vales('PULL',status)

def replica_push_vales(status='normal'):
    print("Replica push de vales ...",datetime.datetime.now())
    try:
        remoteDB = get_remote_pool_connection()
    except Exception as e:
        print(e)
    replica_vales('PUSH', remoteDB, status)

def replica_vales(action, remoteDB,status):
    fecha = datetime.datetime.today()
    table = 'solicitud_de_traslado'
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
    try:
        localDB = get_local_pool_connection()
    except Exception as e:
        print(e)

    try:
        remoteDB = get_remote_pool_connection()
    except Exception as e:
        print(e)
        
    audits = get_audits(localDB, remoteDB,'audit_log' ,action,'solicitud_de_traslado', last_run)
    print(audits)
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
                actualizar_audit(remoteDB,'audit_log',audit['id'], messageReplicated)
            if error:
                 actualizar_audit(remoteDB,'audit_log',audit['id'], 'Error')
        if action == 'PUSH':
            if replicado:
                atiende = get_replica_entity(localDB,'sucursal',sol['sucursal_atiende_id'])
                solicita = get_replica_entity(localDB,'sucursal',sol['sucursal_solicita_id'])
                crear_audit(remoteDB,'OFICINAS', audit,sucursal['nombre'])
                crear_audit(remoteDB,atiende['nombre'], audit,sucursal['nombre'])
                # crear_audit(remoteDB,solicita['nombre'], audit)
                actualizar_audit(localDB,'audit_log',audit['id'],messageReplicated)
            if error:
                actualizar_audit(localDB,'audit_log',audit['id'],'Error')


    if status == "normal":
        create_replica_log(remoteDB,action,sucursal['nombre'],table)

    

def get_sol(localDB,remoteDB,action,id):
    query_sol = f"select * from solicitud_de_traslado where id = '{id}' " 
    query_partidas = f"select * from solicitud_de_traslado_det where solicitud_de_traslado_id = '{id}' " 
    if action == 'PULL':
        sol, partidas =  get_maestro_detalle(remoteDB,query_sol,query_partidas)
        return sol, partidas 
    if action == 'PUSH':
        sol, partidas =  get_maestro_detalle(localDB,query_sol,query_partidas)
        return sol, partidas 
