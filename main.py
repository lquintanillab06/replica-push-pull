from src.jobs.replica_pull import start_pull
from src.jobs.replica_push import start_push
from pynput import keyboard as kb
from pynput.keyboard import Key, Listener







def main():
    print("Cargando App ...")


def replica_run():
    print("Arrancando la Replica!!!")
    start_pull()
    start_push()
    pass
    
    
   
def on_press(key):
    # print('{0} pressed'.format(key))
    if key == Key.enter:
        print("comando no permitido")
    if key == kb.KeyCode.from_char('S'):
        print(" SALIENDO... ")
        print('{0} pressed'.format(key))
        exit()
    if key == kb.KeyCode.from_char('s'):
        print(" SALIENDO... ")
        print('{0} pressed'.format(key))
        exit()
        



if __name__== '__main__':
    # execute only if run as the entry point into the program
   
    main()
    run = True 
    while run :
        print('Running ...')
        replica_run()  
        sent = input("S para salir:  \n") 
        # if(sent.upper() =='S'):
        #     run = False  
       

        # with Listener(on_press=on_press) as listener:
        #     tst = listener.join()
        #     print(tst)
          

            
        kb.Listener(on_press).run()
        


        # if(sent.upper() =='Q'):
        #     replica_run()   
        #     pass
              

           
                

  

