from src.database import get_local_pool_connection,get_remote_pool_connection
from src.services import replica_audit


def replica_pull_producto():
    replica_audit('producto','PULL','audit_log')

def replica_push_producto():
    replica_audit('producto','PUSH','audit',True)