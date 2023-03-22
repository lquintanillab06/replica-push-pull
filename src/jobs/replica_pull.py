from apscheduler.schedulers.background import BackgroundScheduler
from src.operations.replica_existencia import replica_pull_existencia
from src.operations.replica_cliente import replica_pull_cliente
from src.operations.replica_vales import replica_pull_vales
from src.operations.replica_cliente_credito import replica_pull_cliente_credito
from src.operations.replica_cxc import replica_pull_cxc_cod, replica_pull_cxc_con,replica_pull_cxc_cre




def start_pull():
    print("Arrancando el pull job ...")
    scheduler = BackgroundScheduler()
    scheduler.add_job(replica_pull_existencia, 'interval', seconds=45, id='pull_existencia_id')
    scheduler.add_job(replica_pull_cliente, 'interval', seconds=60, id='pull_cliente_id')
    scheduler.add_job(replica_pull_vales, 'interval', seconds=30, id='pull_vales_id')
    scheduler.add_job(replica_pull_cliente_credito, 'interval', seconds=30, id='pull_cliente_credito_id')
    scheduler.add_job(replica_pull_cxc_cod, 'interval', minutes=25, id='pull_cxc_cod')
    scheduler.add_job(replica_pull_cxc_cre, 'interval', minutes=27, id='pull_cxc_cre')
    scheduler.add_job(replica_pull_cxc_con, 'interval', minutes=30, id='pull_cxc_con')

    scheduler.start() 