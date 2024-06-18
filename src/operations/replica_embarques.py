from src.database import get_local_pool_connection,get_remote_pool_connection
from src.services import replica_audit,get_sucursal_local
import datetime


def replica_pull_embarque():
    sucursal = get_sucursal_local()
    if sucursal['nombre'] == 'OFICINAS':  
        print("Replica pull de Embarque ... ",datetime.datetime.now())
        replica_audit('embarque','PULL','audit_log') 
    else:
        print("No se puede ejecutar el pull porque no esta en oficinas")
    
def replica_push_embarque(ValorS = 'normal'):
    print("Replica push de Embarque ...",datetime.datetime.now())
    replica_audit('embarque','PUSH','audit_log', status=ValorS)


def replica_pull_envio():
    sucursal = get_sucursal_local()
    if sucursal['nombre'] == 'OFICINAS':
        print("Replica pull de Envio ... ",datetime.datetime.now())
        replica_audit('envio','PULL','audit_log')  
    else:
        print("No se puede ejecutar el pull porque no esta en oficinas")
    

def replica_push_envio(valor='normal'):
    print("Replica push de Envio ...",datetime.datetime.now())
    replica_audit('envio','PUSH','audit_log', status = valor)


def replica_pull_envio_det():
    sucursal = get_sucursal_local()
    if sucursal['nombre'] == 'OFICINAS':
        print("Replica pull de EnvioDet ... ",datetime.datetime.now())
        replica_audit('envio_det','PULL','audit_log')    
    else:
        print("No se puede ejecutar el pull porque no esta en oficinas")
    

def replica_push_envio_det(valor='normal'):
    print("Replica push de EnvioDet ...",datetime.datetime.now())
    replica_audit('envio_det','PUSH','audit_log', status = valor)