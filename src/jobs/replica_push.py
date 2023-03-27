from apscheduler.schedulers.background import BackgroundScheduler
from src.operations import * 




def start_push():
    print("Arrancando el push job ...")
    scheduler = BackgroundScheduler()
    scheduler.add_job(replica_push_cliente, 'cron',day_of_week= 'mon-sat', hour= '9-19',second='*/20', id='push_cliente_id')
    scheduler.add_job(replica_push_cliente_credito, 'cron',day_of_week= 'mon-sat', hour= '9-19',second='*/20', id='push_cliente_credito_id')
    scheduler.add_job(replica_push_existencia, 'cron',day_of_week= 'mon-sat', hour= '9-19',minute='*/5', id='push_existencia_id')
    scheduler.add_job(replica_push_producto, 'cron',day_of_week= 'mon-sat', hour= '9-19',minute='*/10', id='push_producto_id')
    scheduler.add_job(replica_push_proveedor, 'cron',day_of_week= 'mon-sat', hour= '9-19',minute='35', id='push_proveedor_id')
    scheduler.add_job(replica_push_fichas, 'cron',day_of_week= 'mon-sat', hour= '9-19',minute='11', id='push_fichas_id')

    scheduler.start() 