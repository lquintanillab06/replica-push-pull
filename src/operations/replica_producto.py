from src.database import get_local_pool_connection,get_remote_pool_connection
from src.services import replica_audit


def replica_pull_producto(valors='normal'):
    replica_audit('producto','PULL','audit',status=valors)

def replica_push_producto():
    replica_audit('producto','PUSH','audit',True)