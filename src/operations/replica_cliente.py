from src.database import get_local_pool_connection,get_remote_pool_connection
from src.services import replica_audit
from apscheduler.schedulers.background import BackgroundScheduler
import datetime
import time



def replica_pull_cliente(valors = 'normal'):
    print("Replica pull de Cliente ... ",datetime.datetime.now())
    replica_audit('cliente','PULL','audit_log', status=valors)

def replica_push_cliente():
    print("Replica push de Cliente ...",datetime.datetime.now())
    replica_audit('cliente','PUSH','audit_log',True)

def testprint():    
    print(f"impresion de prueba corrio {datetime.datetime.now()}")
    time.sleep(5)
    # if (datetime.time.hour() == datetime.time()):
    #     'Son la misma hora'
    

def testprint2():
    print(f"    <<<< SOLO CORRE CADA MINUTO  {datetime.datetime.now()}  >>>>>    ")