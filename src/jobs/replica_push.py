from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from src.operations import * 

"""tengo varios sistemas que se repiten cada 2 minutos, otro cada 2 minutos,  3 que se repiten cada 4 minutos , 2 cada 5 minutos, 2 cada 7 minutos, 1 cada 9 minutos, 2 cada 8 minutos,
2 cada 10 minutos, 2 cada 12 minutos, 1 cada 13 minutos, 3 cada 15 minutos , 1 cada 20 minutos y 2 cada 25 minutos
todos inician a las 8 am y terminan a las 7 pm, 
¿cuantas veces se repetirian al mismo tiempo al menos 10 sistemas?"""


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

def start_push():
    print("Arrancando el push job ...")
    scheduler = BackgroundScheduler(job_defaults={'max_instances': 5})
    # """     **********************   funcionando *********************** """
    scheduler.add_job(replica_push_cliente,     'cron', day_of_week= 'mon-sat', hour= '8-19', minute='*/4', id='push_cliente_id')
    scheduler.add_job(replica_push_existencia,  'cron', day_of_week= 'mon-sat', hour= '8-19', minute='*/4', id='push_existencia_id')
    
    # scheduler.add_job(replica_push_fichas,    'cron', day_of_week= 'mon-sat', hour= '9-19', minute='*/25', id='push_fichas_id')
    # scheduler.add_job(replica_push_cxc_con,   'cron', day_of_week= 'mon-sat', hour= '8-19', minute='*/7',  id='push_cxc_con_id')
    # scheduler.add_job(replica_push_cxc_cre,   'cron', day_of_week= 'mon-sat', hour= '8-19', minute='*/10', id='push_cxc_cre_id')
    # scheduler.add_job(replica_push_cxc_cod,   'cron', day_of_week= 'mon-sat', hour= '8-19', minute='*/12', id='push_cxc_cod_id')
    # scheduler.add_job(replica_push_cobros,    'cron', day_of_week= 'mon-sat', hour= '8-19', minute='*/5',  id='push_cobros_id')
    # scheduler.add_job(replica_push_cobranza,  'cron', day_of_week= 'mon-sat', hour= '8-19', minute='*/9',  id='push_cobranza_id')
    scheduler.add_job(replica_push_vales,     'cron', day_of_week= 'mon-sat', hour= '8-19', minute='*/7',  id='push_vales_id')
    scheduler.add_job(replica_push_traslados, 'cron', day_of_week= 'mon-sat', hour= '8-19', minute='*/8',  id='push_traslado_id')
    # scheduler.add_job(replica_push_cancelacion_cfdi,  'cron', day_of_week= 'mon-sat', hour= '8-19', minute='*/15', id='push_cancelacion_cfdi')
    # scheduler.add_job(replica_push_compras,           'cron', day_of_week= 'mon-sat', hour= '9-19', minute='*/25', id='push_compras')
    
    # scheduler.add_job(replica_push_embarque,   'cron', day_of_week= 'mon-sat', hour= '8-19', minute='*/10', id='push_embarques')
    # scheduler.add_job(replica_push_envio,      'cron', day_of_week= 'mon-sat', hour= '8-19', minute='*/12', id='push_envio')
    # scheduler.add_job(replica_push_envio_det,  'cron', day_of_week= 'mon-sat', hour= '8-19', minute='*/13', id='push_envio_det')


    # scheduler.add_job(replica_movimiento_almacen,      'cron', day_of_week= 'mon-sat', hour= '8-19', minute='*/15', id='push_movimiento_almacen')
    # scheduler.add_job(replica_push_transformaciones,   'cron', day_of_week= 'mon-sat', hour= '8-19', minute='*/15', id='push_movimiento_inventario')
    # scheduler.add_job(replica_push_recepcion_compra,   'cron', day_of_week= 'mon-sat', hour= '8-19', minute='*/20', id='push_recepcion_de_compras')
    
    # scheduler.add_job(replica_requisicion_de_material,  'cron', day_of_week= 'mon-sat', hour= '20', minute='30', id='push_requisicion_material')
    # scheduler.add_job(replica_push_morrallas,           'cron', day_of_week= 'mon-sat', hour= '20', minute='2', id='push_morralla')

    """     **********************   funcionando *********************** """

    """    <<<<<<<<    Schedule de revision corren de 1 a 3 veces al dia para revisar de manera general las entidades         >>>>>>>>>>>>>> """

    scheduler.add_job(replica_push_existencia,  'cron', day_of_week= 'mon-sat', hour= '8-21/1', minute='4', id='push_existencia_id_rev', args = ('revision',) )
    # scheduler.add_job(replica_push_cxc_con,  'cron', day_of_week= 'mon-sat', hour= '8-21/4', minute='10', id='push_cxc_con_id_rev',  args = ('revision', ))
    # scheduler.add_job(replica_push_cxc_cre,  'cron', day_of_week= 'mon-sat', hour= '8-21/4', minute='14', id='push_cxc_cre_id_rev',  args = ('revision', ))
    # scheduler.add_job(replica_push_cxc_cod,  'cron', day_of_week= 'mon-sat', hour= '8-21/4', minute='16', id='push_cxc_cod_id_rev',  args = ('revision', ))
    # scheduler.add_job(replica_push_fichas,   'cron', day_of_week= 'mon-sat', hour= '8-21/3', minute='20', id='push_fichas_id_rev',   args = ('revision',))
    # scheduler.add_job(replica_push_cobros,   'cron', day_of_week= 'mon-sat', hour= '8-21/2', minute='30', id='push_cobros_id_rev',   args = ('revision',))
    # scheduler.add_job(replica_push_cobranza, 'cron', day_of_week= 'mon-sat', hour= '8-21/2', minute='40', id='push_cobranza_id_rev', args = ('revision', ))
    scheduler.add_job(replica_push_vales,    'cron', day_of_week= 'mon-sat', hour= '8-21/3', minute='30', id='push_vales_id_rev',    args = ('revision',))
    scheduler.add_job(replica_push_traslados,         'cron', day_of_week= 'mon-sat', hour= '8-21/3', minute='40', id='push_traslado_id_rev',      args = ('revision',))
    # scheduler.add_job(replica_push_cancelacion_cfdi,  'cron', day_of_week= 'mon-sat', hour= '8-21/4', minute='30', id='push_cancelacion_cfdi_rev', args = ('revision',))
    # scheduler.add_job(replica_push_compras,           'cron', day_of_week= 'mon-sat', hour= '8-21/4', minute='40', id='push_compras_rev',          args = ('revision',))

    # scheduler.add_job(replica_push_embarque,  'cron', day_of_week= 'mon-sat', hour= '8-21/2', minute='17', id='push_embarques_rev', args = ('revisar',))
    # scheduler.add_job(replica_push_envio,     'cron', day_of_week= 'mon-sat', hour= '8-21/2', minute='20', id='push_envio_rev',     args = ('revisar',))
    # scheduler.add_job(replica_push_envio_det, 'cron', day_of_week= 'mon-sat', hour= '8-21/2', minute='23', id='push_envio_det_rev', args=('revision',))


    # scheduler.add_job(replica_movimiento_almacen,     'cron', day_of_week= 'mon-sat', hour= '8-21/4', minute='45', id='push_movimiento_almacen_rev',    args=('revision',))
    # scheduler.add_job(replica_push_transformaciones,  'cron', day_of_week= 'mon-sat', hour= '8-21/4', minute='48', id='push_movimiento_inventario_rev', args=('revision',))
    # scheduler.add_job(replica_push_recepcion_compra,  'cron', day_of_week= 'mon-sat', hour= '8-21/4', minute='50', id='push_recepcion_de_compras_rev',  args=('revision',))
    

    """    <<<<<<<<    Schedule de revision corren de 1 a 3 veces al dia para revisar de manera general las entidades         >>>>>>>>>>>>>> """

    def forlist(event):
        y = datetime.datetime.now()
        for i in listdates:
            # print(i)            
            if (i.strftime("%H:%M") == y.strftime("%H:%M")):
                # print("son iguales ")
                # print("pausando ambos jobs")
                scheduler.pause_job('push_fichas_id')
                scheduler.pause_job('push_compras')
                scheduler.pause_job('push_cxc_cre_id')
                scheduler.pause_job('push_cxc_cod_id')                
                scheduler.pause_job('push_recepcion_de_compras')
                scheduler.pause_job('push_cancelacion_cfdi')
                scheduler.pause_job('push_movimiento_almacen')
                scheduler.pause_job('push_movimiento_inventario')
                scheduler.pause_job('push_cxc_cod_id')
                scheduler.pause_job('push_envio')
                scheduler.pause_job('push_embarques')
                scheduler.pause_job('push_cxc_cre_id')
                scheduler.pause_job('push_traslado_id')
                scheduler.pause_job('push_cobros_id')
                scheduler.pause_job('push_cliente_id')
                scheduler.pause_job('push_existencia_id')
                # scheduler.pause_job('idprint2')     
                time.sleep(120)       
                scheduler.resume_job('push_fichas_id')
                scheduler.resume_job('push_compras')
                scheduler.resume_job('push_cxc_cre_id')
                scheduler.resume_job('push_cxc_cod_id')                
                scheduler.resume_job('push_recepcion_de_compras')
                scheduler.resume_job('push_cancelacion_cfdi')
                scheduler.resume_job('push_movimiento_almacen')
                scheduler.resume_job('push_movimiento_inventario')
                scheduler.resume_job('push_cxc_cod_id')
                scheduler.resume_job('push_envio')
                scheduler.resume_job('push_embarques')
                scheduler.resume_job('push_cxc_cre_id')
                scheduler.resume_job('push_traslado_id')
                scheduler.resume_job('push_cobros_id')
                scheduler.resume_job('push_cliente_id')
                scheduler.resume_job('push_existencia_id')
                # scheduler.resume_job('idpint2')
    
    # scheduler.add_listener(forlist, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)



    # scheduler.add_job(replica_audit_push_inventario,  'cron', day_of_week= 'mon-sat', hour= '9-19',minute='*/3', id='push_inv_id')
    # scheduler.add_job(replica_push_cliente_credito, 'cron',day_of_week= 'mon-sat', hour= '9-19',second='*/20', id='push_cliente_credito_id')
    # scheduler.add_job(replica_push_producto, 'cron',day_of_week= 'mon-sat', hour= '9-19',minute='*/10', id='push_producto_id')
    # scheduler.add_job(replica_push_proveedor, 'cron',day_of_week= 'mon-sat', hour= '9-19',minute='35', id='push_proveedor_id')

    scheduler.start() 