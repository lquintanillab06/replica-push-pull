import datetime
from src.services import replica_audit


def replica_pull_cliente_credito():
    print("Replica pull de Cliente Credito ... ",datetime.datetime.now())
    replica_audit('cliente_credito','PULL','audit_log')

def replica_push_cliente_credito():
    print("Replica push de Cliente Credito ... ",datetime.datetime.now())
    replica_audit('cliente_credito','PUSH','audit_log',True)
