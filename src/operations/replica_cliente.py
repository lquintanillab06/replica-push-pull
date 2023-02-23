from src.database import get_local_pool_connection,get_remote_pool_connection
from src.services import replica_audit
import datetime


def replica_pull_cliente():
    print("Replica pull de Cliente ... ",datetime.datetime.now())
    replica_audit('cliente','PULL','audit_log')

def replica_push_cliente():
    print("Replica push de Cliente ...",datetime.datetime.now())
    replica_audit('cliente','PUSH','audit_log',True)
