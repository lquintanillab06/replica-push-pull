from src.database import get_local_pool_connection,get_remote_pool_connection
from src.services import replica_audit


def replica_pull_cliente_credito():
    replica_audit('cliente_credito','PULL','audit')

def replica_push_cliente_credito():
    replica_audit('cliente_credito','PUSH','audit',True)
