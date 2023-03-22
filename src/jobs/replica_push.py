from apscheduler.schedulers.background import BackgroundScheduler
from src.operations.replica_existencia import replica_push_existencia
from src.operations.replica_cliente import replica_push_cliente
from src.operations.replica_vales import replica_push_vales
from src.operations.replica_cliente_credito import replica_push_cliente_credito
from src.operations.replica_cxc import  replica_push_cxc_cod,replica_push_cxc_con,replica_push_cxc_cre




def start_push():
    print("Arrancando el push job ...")
    scheduler = BackgroundScheduler()
    scheduler.add_job(replica_push_existencia, 'interval', minutes=3, id='push_existencia_id')
    scheduler.add_job(replica_push_cliente, 'interval', seconds=20, id='push_cliente_id')
    scheduler.add_job(replica_push_vales, 'interval', minutes=2, id='push_vales_id')
    scheduler.add_job(replica_push_cliente_credito, 'interval', minutes=2, id='push_cliente_credito_id')
    scheduler.add_job(replica_push_cxc_cod, 'interval', minutes=25, id='push_cxc_cod')
    scheduler.add_job(replica_push_cxc_cre, 'interval', minutes=27, id='push_cxc_cre')
    scheduler.add_job(replica_push_cxc_con, 'interval', minutes=30, id='push_cxc_con')
    scheduler.start() 