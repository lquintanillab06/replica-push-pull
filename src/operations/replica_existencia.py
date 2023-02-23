from src.database import get_local_pool_connection,get_remote_pool_connection
from src.services import replica_audit
import datetime

def replica_pull_existencia():
    print("Replica pull de Existencia ...",datetime.datetime.now())
    replica_audit('existencia','PULL','audit_log')

def replica_push_existencia():
    print("Replica push de Existencia ...",datetime.datetime.now())
    replica_audit('existencia','PUSH','audit_log',True)
    
