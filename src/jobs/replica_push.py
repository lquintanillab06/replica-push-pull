from apscheduler.schedulers.background import BackgroundScheduler
from src.operations import * 




def start_push():
    print("Arrancando el push job ...")
    scheduler = BackgroundScheduler(job_defaults={'max_instances': 9})
    # """     **********************   funcionando *********************** """
    scheduler.add_job(replica_push_cliente,     'cron', day_of_week= 'mon-sat', hour= '8-19', minute='*/4', id='push_cliente_id')
    scheduler.add_job(replica_push_existencia,  'cron', day_of_week= 'mon-sat', hour= '8-19', minute='*/4', id='push_existencia_id')
    
    # scheduler.add_job(replica_push_fichas,    'cron', day_of_week= 'mon-sat', hour= '8-19', minute='*/25', id='push_fichas_id')
    # scheduler.add_job(replica_push_cxc_con,   'cron', day_of_week= 'mon-sat', hour= '8-19', minute='*/7',  id='push_cxc_con_id')
    # scheduler.add_job(replica_push_cxc_cre,   'cron', day_of_week= 'mon-sat', hour= '8-19', minute='*/10', id='push_cxc_cre_id')
    # scheduler.add_job(replica_push_cxc_cod,   'cron', day_of_week= 'mon-sat', hour= '8-19', minute='*/12', id='push_cxc_cod_id')
    # scheduler.add_job(replica_push_cobros,    'cron', day_of_week= 'mon-sat', hour= '8-19', minute='*/5',  id='push_cobros_id')
    # scheduler.add_job(replica_push_cobranza,  'cron', day_of_week= 'mon-sat', hour= '8-19', minute='*/9',  id='push_cobranza_id')
    scheduler.add_job(replica_push_vales,     'cron', day_of_week= 'mon-sat', hour= '8-19', minute='*/7',  id='push_vales_id')
    scheduler.add_job(replica_push_traslados, 'cron', day_of_week= 'mon-sat', hour= '8-19', minute='*/8',  id='push_traslado_id')
    # scheduler.add_job(replica_push_cancelacion_cfdi,  'cron', day_of_week= 'mon-sat', hour= '8-19', minute='*/15', id='push_cancelacion_cfdi')
    # scheduler.add_job(replica_push_compras,           'cron', day_of_week= 'mon-sat', hour= '8-19', minute='*/25', id='push_compras')
    
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

    
    
    # scheduler.add_job(replica_audit_push_inventario,  'cron', day_of_week= 'mon-sat', hour= '9-19',minute='*/3', id='push_inv_id')
    # scheduler.add_job(replica_push_cliente_credito, 'cron',day_of_week= 'mon-sat', hour= '9-19',second='*/20', id='push_cliente_credito_id')
    # scheduler.add_job(replica_push_producto, 'cron',day_of_week= 'mon-sat', hour= '9-19',minute='*/10', id='push_producto_id')
    # scheduler.add_job(replica_push_proveedor, 'cron',day_of_week= 'mon-sat', hour= '9-19',minute='35', id='push_proveedor_id')

    scheduler.start() 