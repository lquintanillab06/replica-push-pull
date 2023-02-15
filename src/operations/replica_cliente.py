from src.database import get_local_pool_connection,get_remote_pool_connection
from src.services import replica_audit


def replica_pull_cliente():
    replica_audit('cliente','PULL','audit_log')

def replica_push_cliente():
    replica_audit('cliente','PUSH','audit_log',True)
