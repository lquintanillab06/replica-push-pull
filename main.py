from src.jobs.replica_push import start_push
from src.jobs.replica_pull import start_pull

from src.operations.replica_cxc import replica_push_cxc_con, replica_pull_cxc_con

def main():
    print("Cargando App ...")

def replica_test():
    #start_push()
    #start_pull()
    #replica_pull_cxc_con()
    print("Replica PUSH CXC ...")
    replica_push_cxc_con()
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
           
                

  

