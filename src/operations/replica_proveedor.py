from src.database import get_local_pool_connection,get_remote_pool_connection
from src.services import replica_audit


def replica_pull_proveedor():
    replica_audit('proveedor','PULL','audit_log')

def replica_push_proveedor():
    replica_audit('proveedor','PUSH','audit',True)