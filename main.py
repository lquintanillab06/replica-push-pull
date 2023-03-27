from src.jobs.replica_pull import start_pull
from src.jobs.replica_push import start_push






def main():
    print("Cargando App ...")


def replica_run():
    print("Arrancando la Replica!!!")
    start_pull()
    start_push()
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
            replica_run()   
            pass
              

           
                

  

