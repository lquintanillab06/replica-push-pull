
from src.operations.replica_inventarios import replica_inventario
from src.operations.replica_movimiento_almacen import replica_movimiento_almacen,replica_sucursal_pull_movimiento_almacen
from src.operations.replica_cliente_credito import replica_push_cliente_credito
from src.operations.replica_proveedor import replica_pull_proveedor

from src.test.multiprocess_replica import replica_mp_movimiento_almacen

def main():
    print("Cargando App ...")

def replica_test():
    #replica_inventario('PUSH')
    #replica_movimiento_almacen('PUSH')
    #replica_push_cliente_credito()
    #replica_mp_movimiento_almacen()
    #replica_sucursal_pull_movimiento_almacen()
    #replica_movimiento_almacen('PULL')
    replica_pull_proveedor()

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
           
                

  

