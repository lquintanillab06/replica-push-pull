from apscheduler.schedulers.background import BackgroundScheduler
from src.operations.replica_existencia import replica_push_existencia
from src.operations.replica_cliente import replica_push_cliente
from src.operations.replica_vales import replica_push_vales




def start_push():
    print("Arrancando el push job ...")
    scheduler = BackgroundScheduler()
    scheduler.add_job(replica_push_existencia, 'interval', minutes=3, id='push_existencia_id')
    scheduler.add_job(replica_push_cliente, 'interval', minutes=2, id='push_cliente_id')
    scheduler.add_job(replica_push_vales, 'interval', minutes=2, id='push_vales_id')
    scheduler.start() 