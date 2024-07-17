from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
import time
from src.operations import * 

"""
import datetime
from datetime import time

x = datetime.time(21, 56)
y = datetime.datetime.now()

print(x.strftime("%H:%M"))
print(y.strftime("%H:%M"))


if (x.strftime("%H:%M") == y.strftime("%H:%M")):
	print("Son iguales")
else:
	print("Son diferentes")

    
"""
""" 
listdates = [ 
datetime.time(8,20), datetime.time(8,30), datetime.time(8,40),
datetime.time(9,0), datetime.time(9,10), datetime.time(9,12), datetime.time(9,20), datetime.time(9,30), datetime.time(9,40),
datetime.time(10,0), datetime.time(10,20), datetime.time(10,24), datetime.time(10,30), datetime.time(10,40), datetime.time(10,48),
datetime.time(11,0), datetime.time(11,12), datetime.time(11,20), datetime.time(11,30), datetime.time(11,36), datetime.time(11,40),
datetime.time(12,0), datetime.time(12,12), datetime.time(12,20), datetime.time(12,30), datetime.time(12,40), datetime.time(12,48),
datetime.time(13,0), datetime.time(13,12), datetime.time(13,15), datetime.time(13,20), datetime.time(13,30), datetime.time(13,36), datetime.time(13,40), datetime.time(13,50),
datetime.time(14,0), datetime.time(14,20), datetime.time(14,24), datetime.time(14,30), datetime.time(14,40),
datetime.time(15,0), datetime.time(15,12), datetime.time(15,20), datetime.time(15,28), datetime.time(15,30), datetime.time(15,40), datetime.time(15,48),
datetime.time(16,0), datetime.time(16,10), datetime.time(16,20), datetime.time(16,24), datetime.time(16,30), datetime.time(16,40), datetime.time(16,45),
datetime.time(17,0), datetime.time(17,20), datetime.time(17,30), datetime.time(17,36), datetime.time(17,40),
datetime.time(18,0), datetime.time(18,20), datetime.time(18,24), datetime.time(18,30), datetime.time(18,40), datetime.time(18,48),
datetime.time(19,0)
]"""

listdates = [ 
datetime.time(8,19), datetime.time(8,29), datetime.time(8,39),
datetime.time(8,59), datetime.time(9,9), datetime.time(9,11), datetime.time(9,19), datetime.time(9,29), datetime.time(9,39),
datetime.time(9,59), datetime.time(10,19), datetime.time(10,23), datetime.time(10,29), datetime.time(10,39), datetime.time(10,47),
datetime.time(10,59), datetime.time(11,11), datetime.time(11,19), datetime.time(11,29), datetime.time(11,35), datetime.time(11,39),
datetime.time(11,59), datetime.time(12,11), datetime.time(12,19), datetime.time(12,29), datetime.time(12,39), datetime.time(12,47),
datetime.time(12,59), datetime.time(13,11), datetime.time(13,14), datetime.time(13,19), datetime.time(13,29), datetime.time(13,35), 
datetime.time(13,39), datetime.time(13,49),
datetime.time(13,59), datetime.time(14,19), datetime.time(14,23), datetime.time(14,29), datetime.time(14,39),
datetime.time(14,59), datetime.time(15,11), datetime.time(15,19), datetime.time(15,27), datetime.time(15,29), datetime.time(15,39),
datetime.time(15,47),
datetime.time(15,59), datetime.time(16,9), datetime.time(16,19), datetime.time(16,23), datetime.time(16,29), datetime.time(16,39), 
datetime.time(16,44),
datetime.time(16,59), datetime.time(17,19), datetime.time(17,29), datetime.time(17,35), datetime.time(17,39),
datetime.time(17,59), datetime.time(18,19), datetime.time(18,23), datetime.time(18,29), datetime.time(18,39), datetime.time(18,47),
datetime.time(18,59)
]

def start_pull():
    print("Arrancando el pull job ...")
    # scheduler = BackgroundScheduler(executors={'default':ThreadPoolExecutor(10)})
    scheduler = BackgroundScheduler(job_defaults={'max_instances': 6 })
    # scheduler.add_job(replica_pull_cliente,         'cron',day_of_week= 'mon-sat', hour= '9-19',minute='*/2', id='pull_cliente_id')
    # scheduler.add_job(replica_pull_cliente_credito, 'cron',day_of_week= 'mon-sat', hour= '8-19',minute='*/4', id='pull_client_credito_id')
    scheduler.add_job(replica_pull_existencia,      'cron',day_of_week= 'mon-sat', hour= '8-19',minute='*/5', id='pull_existencia_id')
    scheduler.add_job(replica_pull_vales,           'cron',day_of_week= 'mon-sat', hour= '8-19',minute='*/5', id='pull_vales_id')
    scheduler.add_job(replica_pull_traslados,       'cron',day_of_week= 'mon-sat', hour= '8-19', minute='*/8',  id='pull_traslado_id')
    # scheduler.add_job(replica_pull_producto,        'cron',day_of_week= 'mon-sat', hour= '8-19',minute='*/10', id='pull_producto_id')
    # scheduler.add_job(replica_pull_proveedor,       'cron',day_of_week= 'mon-sat', hour= '9-19',minute='30', id='pull_proveedor_id')
    # scheduler.add_job(replica_pull_compras,         'cron',day_of_week= 'mon-sat', hour= '8-19',minute='*/15', id='pull_compras_id')
    

    """     <<<<<  JOBS DE REVISION  >>>>>     """
    # scheduler.add_job(replica_pull_cliente, 'cron',day_of_week= 'mon-sat', hour= '8-19/1',minute='30', id='pull_cliente_id_rev', args = ('revision',))
    # scheduler.add_job(replica_pull_cliente_credito, 'cron',day_of_week= 'mon-sat', hour= '8-19',minute='*/30', id='pull_client_credito_id_rev', args = ('revision',))
    scheduler.add_job(replica_pull_existencia, 'cron',day_of_week= 'mon-sat', hour= '8-19',minute='*/30', id='pull_existencia_id_rev', args = ('revision',))
    scheduler.add_job(replica_pull_vales, 'cron',day_of_week= 'mon-sat', hour= '8-19',minute='*/30', id='pull_vales_id_rev', args = ('revision',))
    scheduler.add_job(replica_pull_traslados, 'cron',day_of_week= 'mon-sat', hour= '8-19',minute='*/30', id='pull_traslado_id_rev', args = ('revision',))
    
    # scheduler.add_job(replica_pull_producto, 'cron',day_of_week= 'mon-sat', hour= '8-19',minute='*/30', id='pull_producto_id_rev', args = ('revision',))
    # scheduler.add_job(replica_pull_proveedor, 'cron',day_of_week= 'mon-sat', hour= '8-19',minute='*/30', id='pull_proveedor_id_rev', args = ('revision',))
    # scheduler.add_job(replica_pull_compras, 'cron',day_of_week= 'mon-sat', hour= '8-19',minute='*/30', id='pull_compras_id_rev', args = ('revision',))
    
    # scheduler.add_job(testprint, 'cron', second='*/5', id='idprint')

    def forlistpull(event):
        y = datetime.datetime.now()
        for i in listdates:           
            if (i.strftime("%H:%M") == y.strftime("%H:%M")):
                scheduler.pause_job('pull_cliente_id')
                scheduler.pause_job('pull_client_credito_id')
                scheduler.pause_job('pull_existencia_id')
                scheduler.pause_job('pull_vales_id')                
                scheduler.pause_job('pull_traslado_id')
                scheduler.pause_job('pull_producto_id')
                scheduler.pause_job('pull_proveedor_id')
                scheduler.pause_job('pull_compras_id')
                scheduler.pause_job('pull_cliente_id_rev')
                scheduler.pause_job('pull_client_credito_id_rev')
                scheduler.pause_job('pull_existencia_id_rev')
                scheduler.pause_job('pull_vales_id_rev')
                scheduler.pause_job('pull_traslado_id_rev')
                scheduler.pause_job('pull_producto_id_rev')
                scheduler.pause_job('pull_proveedor_id_rev')
                scheduler.pause_job('pull_compras_id_rev')
                time.sleep(120)    
                scheduler.resume_job('pull_cliente_id')
                scheduler.resume_job('pull_client_credito_id')
                scheduler.resume_job('pull_existencia_id')
                scheduler.resume_job('pull_vales_id')                
                scheduler.resume_job('pull_traslado_id')
                scheduler.resume_job('pull_producto_id')
                scheduler.resume_job('pull_proveedor_id')
                scheduler.resume_job('pull_compras_id')
                scheduler.resume_job('pull_cliente_id_rev')
                scheduler.resume_job('pull_client_credito_id_rev')
                scheduler.resume_job('pull_existencia_id_rev')
                scheduler.resume_job('pull_vales_id_rev')
                scheduler.resume_job('pull_traslado_id_rev')
                scheduler.resume_job('pull_producto_id_rev')
                scheduler.resume_job('pull_proveedor_id_rev')
                scheduler.resume_job('pull_compras_id_rev')

    # scheduler.add_listener(forlistpull, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)

    scheduler.start() 

# def start_pull_pausing():
#     print("funcionando")
#     pullscheduler = BackgroundScheduler(job_defaults={'max_instances': 10})

#     pullscheduler.start()
