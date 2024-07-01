from apscheduler.schedulers.background import BackgroundScheduler
from src.operations import * 





def start_pull():
    print("Arrancando el pull job ...")
    scheduler = BackgroundScheduler(job_defaults={'max_instances': 10})
    scheduler.add_job(replica_pull_cliente, 'cron',day_of_week= 'mon-sat', hour= '9-19',minute='*/2', id='pull_cliente_id')
    scheduler.add_job(replica_pull_cliente_credito, 'cron',day_of_week= 'mon-sat', hour= '8-19',minute='*/4', id='pull_client_credito_id')
    scheduler.add_job(replica_pull_existencia, 'cron',day_of_week= 'mon-sat', hour= '8-19',minute='*/5', id='pull_existencia_id')
    scheduler.add_job(replica_pull_vales, 'cron',day_of_week= 'mon-sat', hour= '8-19',minute='*/5', id='pull_vales_id')
    scheduler.add_job(replica_pull_traslados, 'cron', day_of_week= 'mon-sat', hour= '8-19', minute='*/8',  id='pull_traslado_id')
    
    

    # scheduler.add_job(replica_pull_producto, 'cron',day_of_week= 'mon-sat', hour= '9-19',minute='*/10', id='pull_producto_id')
    # scheduler.add_job(replica_pull_proveedor, 'cron',day_of_week= 'mon-sat', hour= '9-19',minute='30', id='pull_proveedor_id')
    # REPLICA COMPRAS
    # DEVOLUCION DE COMPRAS
    # PROVEEDOR PRODUCTO 
    
    
    # scheduler.add_job(replica_pull_fichas, 'cron',day_of_week= 'mon-sat', hour= '9-19',minute='13', id='pull_fichas_id')


    """  Scheduler que revisa la info completa del dia  """




    scheduler.start() 