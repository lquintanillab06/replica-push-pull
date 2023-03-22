import src.services.logger_base
from src.jobs.replica_push import start_push
from src.jobs.replica_pull import start_pull

from src.operations.replica_cxc import replica_push_cxc_con, replica_pull_cxc_con
from src.validations.validar_inventarios import validar_inventario_hoy,validar_inventario_fecha
from src.operations.replica_inventarios import replica_audit_pull_inventario
from src.operations.replica_movimientos_inventario import replica_push_movimientos, replica_pull_movimientos
from src.operations.replica_fichas import replica_push_fichas,replica_pull_fichas
from src.operations.replica_morrallas import replica_push_morrallas, replica_pull_morrallas
from src.operations.replica_cobranza import replica_push_cobranza,replica_pull_cobranza
from src.operations.replica_requisicion_material import replica_requisicion_de_material
from src.operations.replica_compras import replica_push_compras, replica_pull_compras
from src.operations.replica_recepcion_compra import replica_push_recepcion_compra, replica_pull_recepcion_compra
from src.operations.replica_devolucion_compra import replica_pull_devolucion_compra,replica_push_devolucion_compra

from src.database import get_local_pool_connection,get_remote_pool_connection

from src.services.email_service import enviar_email, envio_masivo, envio_masivo_db

def main():
    
    print("Cargando App ...")


def replica_test():
    print("Arrancando la Replica!!!")
    #replica_push_fichas()
    #replica_pull_fichas()
    #replica_push_morrallas()
    #replica_pull_morrallas()
    #replica_push_cobranza()
    #replica_pull_cobranza()
    #replica_pull_movimientos('transformacion','transformacion_det')
    #replica_push_movimientos('transformacion','transformacion_det')
    #replica_push_movimientos('movimiento_de_almacen','movimiento_de_almacen_det')
    #replica_push_movimientos('devolucion_de_venta','devolucion_de_venta_det')
    #validar_inventario_hoy()
    #replica_audit_pull_inventario()
    #validar_inventario_fecha('2023/03/03')
    #start_push()
    #start_pull()
    #replica_pull_cxc_con()
    #print("Replica PUSH CXC ...")
    #replica_push_cxc_con()
    #replica_requisicion_de_material()
    #replica_push_compras()
    #replica_pull_compras()
    #replica_push_recepcion_compra()
    #replica_pull_recepcion_compra()
    #replica_pull_devolucion_compra()
    envio_masivo_db()
    #enviar_email('jass@papelsa.com.mx')
    #envio_masivo_db()

    pass
    
    
    

if __name__== '__main__':
    # execute only if run as the entry point into the program
   
    main()
    run = True 
    while run :
        print('Running ...')
        sent = input("S para salir:  \n") 
        if(sent.upper() =='S'):
            run = False  

        if(sent.upper() =='Q'):
            replica_test()   
            pass
              

           
                

  

